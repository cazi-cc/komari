package utils

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/internal/scheduler"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
)

const tcpQualityMaxParallel = 4

type tcpQualityTaskManager struct {
	mu       sync.Mutex
	inFlight map[string]tcpQualityInFlight
}

type tcpQualityInFlight struct {
	runID     string
	expiresAt time.Time
}

var tcpQualityManager = &tcpQualityTaskManager{
	inFlight: make(map[string]tcpQualityInFlight),
}

func ReloadTCPQualitySchedule(taskList []models.TCPQualityTask) error {
	scheduler.RemovePrefix("tcp-quality:task:")
	groups := make(map[int][]models.TCPQualityTask)
	for _, task := range taskList {
		if !task.Enabled || task.Interval <= 0 {
			continue
		}
		groups[task.Interval] = append(groups[task.Interval], task)
	}
	for interval, grouped := range groups {
		interval := interval
		grouped := append([]models.TCPQualityTask(nil), grouped...)
		name := fmt.Sprintf("tcp-quality:task:%d", interval)
		if err := scheduler.AddContextFunc(name, scheduler.Every(time.Duration(interval)*time.Second), false, func(ctx context.Context) {
			for _, task := range grouped {
				task := task
				go func() {
					_ = ExecuteTCPQualityTask(ctx, task)
				}()
			}
		}); err != nil {
			return err
		}
	}
	return nil
}

func ExecuteTCPQualityTask(ctx context.Context, task models.TCPQualityTask) error {
	if !task.Enabled {
		return fmt.Errorf("TCP quality task is disabled")
	}
	catalog, err := loadTCPQualityCatalog(ctx, false)
	if err != nil {
		return err
	}
	targets := selectTCPQualityTargets(task, catalog)
	if len(targets) == 0 {
		return fmt.Errorf("TCP quality task has no available catalog targets")
	}
	for _, clientUUID := range task.Clients {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		runID := newTCPQualityRunID()
		if !beginTCPQualityRun(task, clientUUID, runID, len(targets)) {
			continue
		}
		params := v2.TCPQualityParams{
			TaskID:          task.Id,
			RunID:           runID,
			CatalogRevision: catalog.View.Revision,
			Targets:         append([]v2.TCPQualityTarget(nil), targets...),
			StandardPackets: task.StandardPackets,
			LargeEnabled:    task.LargeEnabled,
			LargePackets:    task.LargePackets,
			DelayMS:         task.DelayMS,
			TimeoutMS:       task.TimeoutMS,
			MaxParallel:     tcpQualityMaxParallel,
		}
		if !agent_runtime.DispatchV2Event(clientUUID, v2.MethodAgentTCPQuality, params) {
			CompleteTCPQualityRun(task.Id, clientUUID, runID)
		}
	}
	return nil
}

func beginTCPQualityRun(task models.TCPQualityTask, clientUUID, runID string, targetCount int) bool {
	key := tcpQualityRunKey(task.Id, clientUUID)
	now := time.Now().UTC()
	packetCount := targetCount * task.StandardPackets
	if task.LargeEnabled {
		packetCount += targetCount * task.LargePackets
	}
	estimated := time.Duration(packetCount) * time.Duration(task.DelayMS+task.TimeoutMS) * time.Millisecond / tcpQualityMaxParallel
	if estimated < 15*time.Minute {
		estimated = 15 * time.Minute
	}
	if estimated > 12*time.Hour {
		estimated = 12 * time.Hour
	}

	tcpQualityManager.mu.Lock()
	defer tcpQualityManager.mu.Unlock()
	if current, exists := tcpQualityManager.inFlight[key]; exists && current.expiresAt.After(now) {
		return false
	}
	tcpQualityManager.inFlight[key] = tcpQualityInFlight{
		runID:     runID,
		expiresAt: now.Add(estimated),
	}
	return true
}

func CompleteTCPQualityRun(taskID uint, clientUUID, runID string) {
	key := tcpQualityRunKey(taskID, clientUUID)
	tcpQualityManager.mu.Lock()
	defer tcpQualityManager.mu.Unlock()
	if current, exists := tcpQualityManager.inFlight[key]; exists && current.runID == runID {
		delete(tcpQualityManager.inFlight, key)
	}
}

func tcpQualityRunKey(taskID uint, clientUUID string) string {
	return fmt.Sprintf("%d:%s", taskID, clientUUID)
}

func newTCPQualityRunID() string {
	var random [12]byte
	if _, err := rand.Read(random[:]); err == nil {
		return hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("%x", time.Now().UTC().UnixNano())
}
