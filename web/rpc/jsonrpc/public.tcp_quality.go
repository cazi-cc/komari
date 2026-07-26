package jsonrpc

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/pkg/rpc"
)

type tcpQualityTaskID uint

func (id *tcpQualityTaskID) UnmarshalJSON(data []byte) error {
	if len(data) > 0 && data[0] == '"' {
		var value string
		if err := json.Unmarshal(data, &value); err != nil {
			return err
		}
		parsed, err := strconv.ParseUint(value, 10, 0)
		if err != nil {
			return err
		}
		*id = tcpQualityTaskID(parsed)
		return nil
	}

	var value uint
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	*id = tcpQualityTaskID(value)
	return nil
}

func init() {
	regPublic("getPublicTCPQualityTasks", publicGetTCPQualityTasks, "List public TCP quality task labels")
	regPublic("getPublicTCPQualitySnapshot", publicGetTCPQualitySnapshot, "Read a precomputed TCP quality snapshot")
}

func publicGetTCPQualityTasks(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	taskList, err := tasks.GetAllTCPQualityTasks()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	type publicTask struct {
		ID            uint     `json:"id"`
		Name          string   `json:"name"`
		ProvinceCodes []string `json:"province_codes"`
		ISPCodes      []string `json:"isp_codes"`
		IPVersions    []string `json:"ip_versions"`
		LargeEnabled  bool     `json:"large_enabled"`
	}
	result := make([]publicTask, 0, len(taskList))
	for _, task := range taskList {
		if !task.Enabled {
			continue
		}
		result = append(result, publicTask{
			ID:            task.Id,
			Name:          task.Name,
			ProvinceCodes: append([]string(nil), task.ProvinceCodes...),
			ISPCodes:      append([]string(nil), task.ISPCode...),
			IPVersions:    append([]string(nil), task.IPVersions...),
			LargeEnabled:  task.LargeEnabled,
		})
	}
	return result, nil
}

func publicGetTCPQualitySnapshot(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
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
	payload, _, err := tasks.GetTCPQualitySnapshot(uint(params.TaskID), params.WindowHours)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "TCP quality snapshot is not ready", nil)
	}
	var result any
	if err := json.Unmarshal(payload, &result); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "TCP quality snapshot is invalid", nil)
	}
	return result, nil
}
