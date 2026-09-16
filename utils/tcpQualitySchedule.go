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
	for _, task := range taskList {
		if !task.Enabled || task.Interval <= 0 {
			continue
		}
		task := task
		name := fmt.Sprintf("tcp-quality:task:%d", task.Id)
		if err := scheduler.AddContextFuncAtPhase(name, time.Duration(task.Interval)*time.Second, time.Duration(task.SchedulePhaseMS)*time.Millisecond, func(ctx context.Context) {
			_ = ExecuteTCPQualityTask(ctx, task)
		}); err != nil {
			return err
		}
	}
	return nil
}

func ExecuteTCPQualityTask(ctx context.Context, task models.TCPQualityTask) error {
	return executeTCPQualityTask(ctx, task, false)
}

func ExecuteTCPQualityTaskForced(ctx context.Context, task models.TCPQualityTask) error {
	return executeTCPQualityTask(ctx, task, true)
}

func executeTCPQualityTask(ctx context.Context, task models.TCPQualityTask, forceExperimental bool) error {
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
	experimentalDue := task.LargeEnabled && (forceExperimental || tcpQualityExperimentalDue(task, time.Now().UTC()))
	parallel := tcpQualityMaxParallel
	if experimentalDue {
		parallel = 1
	}
	for _, clientUUID := range task.Clients {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		runID := newTCPQualityRunID()
		if !beginTCPQualityRun(task, clientUUID, runID, len(targets), experimentalDue) {
			continue
		}
		params := v2.TCPQualityParams{
			TaskID:          task.Id,
			RunID:           runID,
			CatalogRevision: catalog.View.Revision,
			Targets:         append([]v2.TCPQualityTarget(nil), targets...),
			StandardPackets: task.StandardPackets,
			// Old agents ignore the v6 fields below. Keeping this false prevents
			// them from running the legacy mixed-payload probe during rollout.
			LargeEnabled:               false,
			LargePackets:               task.LargePackets,
			ExperimentalEnabled:        task.LargeEnabled,
			ExperimentalDue:            experimentalDue,
			ExperimentalPackets:        task.LargePackets,
			ExperimentalControlPackets: 5,
			DelayMS:                    task.DelayMS,
			TimeoutMS:                  task.TimeoutMS,
			MaxParallel:                parallel,
		}
		if !agent_runtime.DispatchV2Event(clientUUID, v2.MethodAgentTCPQuality, params) {
			CompleteTCPQualityRun(task.Id, clientUUID, runID)
		}
	}
	return nil
}

func beginTCPQualityRun(task models.TCPQualityTask, clientUUID, runID string, targetCount int, experimentalDue bool) bool {
	key := tcpQualityRunKey(task.Id, clientUUID)
	now := time.Now().UTC()
	packetCount := targetCount * task.StandardPackets
	if experimentalDue {
		packetCount += targetCount * task.LargePackets * 3
		packetCount += 5
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

func tcpQualityExperimentalDue(task models.TCPQualityTask, now time.Time) bool {
	interval := task.Interval
	if interval < 1 {
		return false
	}
	experimentalInterval := task.ExperimentalInterval
	if experimentalInterval < interval {
		experimentalInterval = interval
	}
	every := (experimentalInterval + interval - 1) / interval
	if every <= 1 {
		return true
	}
	phaseMS := task.SchedulePhaseMS
	if phaseMS < 0 {
		phaseMS = 0
	}
	slot := (now.UnixMilli() - phaseMS) / int64(interval*1000)
	return slot%int64(every) == 0
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
