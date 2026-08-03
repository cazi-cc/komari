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

type unlockQualityTaskManager struct {
	mu         sync.Mutex
	inFlight   map[string]tcpQualityInFlight
	lastVerify map[string]time.Time
}

var unlockQualityManager = &unlockQualityTaskManager{
	inFlight:   make(map[string]tcpQualityInFlight),
	lastVerify: make(map[string]time.Time),
}

func ReloadUnlockQualitySchedule(taskList []models.UnlockQualityTask) error {
	scheduler.RemovePrefix("unlock-quality:task:")
	for _, task := range taskList {
		if !task.Enabled || task.Interval <= 0 {
			continue
		}
		task := task
		name := fmt.Sprintf("unlock-quality:task:%d", task.Id)
		if err := scheduler.AddContextFuncAtPhase(name, time.Duration(task.Interval)*time.Second, time.Duration(task.SchedulePhaseMS)*time.Millisecond, func(ctx context.Context) {
			_ = ExecuteUnlockQualityTask(ctx, task, false)
		}); err != nil {
			return err
		}
	}
	return nil
}

func ExecuteUnlockQualityTask(ctx context.Context, task models.UnlockQualityTask, forceVerify bool) error {
	if !task.Enabled {
		return fmt.Errorf("unlock quality task is disabled")
	}
	type route struct {
		mode         string
		dnsServer    string
		fixedAddress string
	}
	routes := []route{{mode: "system"}}
	if task.ControlEnabled {
		routes = append(routes, route{mode: "control", dnsServer: task.ControlDNS})
	}
	if task.FixedEnabled {
		routes = append(routes, route{mode: "fixed", fixedAddress: task.FixedAddress})
	}
	for _, clientUUID := range task.Clients {
		for _, selectedRoute := range routes {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			probeKind := "minute"
			if forceVerify || unlockQualityVerifyDue(task, clientUUID, selectedRoute.mode) {
				probeKind = "verify"
			}
			runID := newUnlockQualityRunID()
			if !beginUnlockQualityRun(task, clientUUID, selectedRoute.mode, runID) {
				continue
			}
			params := v2.UnlockQualityParams{
				TaskID:          task.Id,
				RunID:           runID,
				Service:         task.Service,
				CatalogRevision: "chatgpt_v1",
				RouteMode:       selectedRoute.mode,
				ProbeKind:       probeKind,
				DNSServer:       selectedRoute.dnsServer,
				FixedAddress:    selectedRoute.fixedAddress,
				SampleCount:     task.SampleCount,
				TimeoutMS:       task.TimeoutMS,
			}
			if !agent_runtime.DispatchV2Event(clientUUID, v2.MethodAgentUnlockQuality, params) {
				CompleteUnlockQualityRun(task.Id, clientUUID, selectedRoute.mode, runID)
				continue
			}
			if probeKind == "verify" {
				markUnlockQualityVerified(task.Id, clientUUID, selectedRoute.mode)
			}
		}
	}
	return nil
}

func unlockQualityVerifyDue(task models.UnlockQualityTask, clientUUID, routeMode string) bool {
	key := unlockQualityRunKey(task.Id, clientUUID, routeMode)
	now := time.Now().UTC()
	unlockQualityManager.mu.Lock()
	defer unlockQualityManager.mu.Unlock()
	last := unlockQualityManager.lastVerify[key]
	return last.IsZero() || now.Sub(last) >= time.Duration(task.VerifyInterval)*time.Second
}

func markUnlockQualityVerified(taskID uint, clientUUID, routeMode string) {
	key := unlockQualityRunKey(taskID, clientUUID, routeMode)
	unlockQualityManager.mu.Lock()
	unlockQualityManager.lastVerify[key] = time.Now().UTC()
	unlockQualityManager.mu.Unlock()
}

func beginUnlockQualityRun(task models.UnlockQualityTask, clientUUID, routeMode, runID string) bool {
	key := unlockQualityRunKey(task.Id, clientUUID, routeMode)
	now := time.Now().UTC()
	estimated := time.Duration(task.TimeoutMS*6+10000) * time.Millisecond
	if estimated < 45*time.Second {
		estimated = 45 * time.Second
	}
	if estimated > 4*time.Minute {
		estimated = 4 * time.Minute
	}
	unlockQualityManager.mu.Lock()
	defer unlockQualityManager.mu.Unlock()
	if current, exists := unlockQualityManager.inFlight[key]; exists && current.expiresAt.After(now) {
		return false
	}
	unlockQualityManager.inFlight[key] = tcpQualityInFlight{
		runID:     runID,
		expiresAt: now.Add(estimated),
	}
	return true
}

func CompleteUnlockQualityRun(taskID uint, clientUUID, routeMode, runID string) {
	key := unlockQualityRunKey(taskID, clientUUID, routeMode)
	unlockQualityManager.mu.Lock()
	defer unlockQualityManager.mu.Unlock()
	if current, exists := unlockQualityManager.inFlight[key]; exists && current.runID == runID {
		delete(unlockQualityManager.inFlight, key)
	}
}

func unlockQualityRunKey(taskID uint, clientUUID, routeMode string) string {
	return fmt.Sprintf("%d:%s:%s", taskID, clientUUID, routeMode)
}

func newUnlockQualityRunID() string {
	var random [12]byte
	if _, err := rand.Read(random[:]); err == nil {
		return hex.EncodeToString(random[:])
	}
	return fmt.Sprintf("%x", time.Now().UTC().UnixNano())
}
