package jsonrpc

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/pkg/rpc"
)

func init() {
	regPublic("getPublicUnlockQualityTasks", publicGetUnlockQualityTasks, "List public unlock quality task labels")
	regPublic("getPublicUnlockQualitySnapshot", publicGetUnlockQualitySnapshot, "Read a precomputed unlock quality snapshot")
}

func publicGetUnlockQualityTasks(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	taskList, err := tasks.GetAllUnlockQualityTasks()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	type publicTask struct {
		ID      uint   `json:"id"`
		Name    string `json:"name"`
		Service string `json:"service"`
	}
	result := make([]publicTask, 0, len(taskList))
	for _, task := range taskList {
		if task.Enabled {
			name := "解锁线路"
			if task.Service == "chatgpt" {
				name = "ChatGPT"
			}
			result = append(result, publicTask{ID: task.Id, Name: name, Service: task.Service})
		}
	}
	return result, nil
}

func publicGetUnlockQualitySnapshot(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		TaskID      tcpQualityTaskID `json:"task_id"`
		WindowHours int              `json:"window_hours"`
		Hours       string           `json:"hours"`
	}
	if err := req.BindParams(&params); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	if params.WindowHours == 0 && params.Hours != "" {
		if hours, ok := parseTCPQualityWindow(params.Hours); ok {
			params.WindowHours = hours
		}
	}
	if _, ok := parseTCPQualityWindow(strconv.Itoa(params.WindowHours)); !ok || params.TaskID == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "task_id and a supported window_hours are required", nil)
	}
	task, err := tasks.GetUnlockQualityTask(uint(params.TaskID))
	if err != nil || !task.Enabled {
		return nil, rpc.MakeError(rpc.InvalidParams, "unlock quality task is not available", nil)
	}
	payload, err := tasks.GetUnlockQualitySnapshot(uint(params.TaskID), params.WindowHours)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "unlock quality snapshot is not ready", nil)
	}
	var result any
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "unlock quality snapshot is invalid", nil)
	}
	return result, nil
}
