package utils

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/internal/scheduler"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
)

// PingTaskManager 管理定时器和任务
type PingTaskManager struct {
	mu    sync.Mutex
	tasks map[int][]models.PingTask
}

var manager = &PingTaskManager{
	tasks: make(map[int][]models.PingTask),
}

// Reload 重载时间表
func (m *PingTaskManager) Reload(pingTasks []models.PingTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	scheduler.RemovePrefix("ping:")
	m.tasks = make(map[int][]models.PingTask)

	// 按Interval分组任务
	taskGroups := make(map[int][]models.PingTask)
	for _, task := range pingTasks {
		if task.Interval <= 0 {
			continue
		}
		taskGroups[task.Interval] = append(taskGroups[task.Interval], task)
	}

	// 为每个唯一的Interval创建协程
	for interval, tasks := range taskGroups {
		interval := interval
		tasks := append([]models.PingTask(nil), tasks...)
		m.tasks[interval] = tasks
		taskIDs := make([]uint, 0, len(tasks))
		for _, task := range tasks {
			taskIDs = append(taskIDs, task.Id)
		}
		delays := probeScheduleDelays("ping", interval, taskIDs)
		for _, task := range tasks {
			task := task
			name := fmt.Sprintf("ping:task:%d", task.Id)
			if err := scheduler.AddContextFuncWithStartDelay(name, scheduler.Every(time.Duration(interval)*time.Second), delays[task.Id], func(ctx context.Context) {
				executePingTask(ctx, task)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// executePingTask 执行单个PingTask
func executePingTask(ctx context.Context, task models.PingTask) {
	var message struct {
		TaskID  uint            `json:"ping_task_id"`
		Message string          `json:"message"`
		Type    string          `json:"ping_type"`
		Target  string          `json:"ping_target"`
		Options v2.ProbeOptions `json:"ping_options,omitempty"`
	}

	message.Message = "ping"
	message.TaskID = task.Id
	message.Type = task.Type
	message.Target = task.Target
	message.Options = v2.ProbeOptions{
		PacketSize:       task.ProbeConfig.PacketSize,
		SampleCount:      task.ProbeConfig.SampleCount,
		TimeoutMS:        task.ProbeConfig.TimeoutMS,
		DNSServer:        task.ProbeConfig.DNSServer,
		PreferredIP:      task.ProbeConfig.PreferredIP,
		ValidStatusCodes: append([]int(nil), task.ProbeConfig.ValidStatusCodes...),
	}

	for _, clientUUID := range targetPingClientUUIDs(task) {
		select {
		case <-ctx.Done():
			// Context was canceled, stop sending pings.
			return
		default:
			// Context is still active, continue.
		}

		agent_runtime.DispatchPing(clientUUID, message, v2.PingParams{
			TaskID:  task.Id,
			Type:    task.Type,
			Target:  task.Target,
			Options: message.Options,
		})
	}
}

// targetPingClientUUIDs 根据任务配置计算本次调度需要下发的在线服务器列表。
func targetPingClientUUIDs(task models.PingTask) []string {
	return task.Clients
}

// ReloadPingSchedule 加载或重载时间表
func ReloadPingSchedule(pingTasks []models.PingTask) error {
	return manager.Reload(pingTasks)
}
