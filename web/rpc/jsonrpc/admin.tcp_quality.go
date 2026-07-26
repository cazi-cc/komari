package jsonrpc

import (
	"context"
	"strconv"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/pkg/rpc"
	"github.com/komari-monitor/komari/utils"
)

func init() {
	regAdminTCPQuality("getTCPQualityCatalog", adminGetTCPQualityCatalog, "Get the sanitized TCP quality target catalog")
	regAdminTCPQuality("refreshTCPQualityCatalog", adminRefreshTCPQualityCatalog, "Refresh the TCP quality target catalog")
	regAdminTCPQuality("getTCPQualityTasks", adminGetTCPQualityTasks, "List TCP quality tasks")
	regAdminTCPQuality("addTCPQualityTask", adminAddTCPQualityTask, "Create a TCP quality task")
	regAdminTCPQuality("editTCPQualityTask", adminEditTCPQualityTask, "Edit a TCP quality task")
	regAdminTCPQuality("deleteTCPQualityTasks", adminDeleteTCPQualityTasks, "Delete TCP quality tasks")
	regAdminTCPQuality("runTCPQualityTaskNow", adminRunTCPQualityTaskNow, "Dispatch a TCP quality task now")
	regAdminTCPQuality("refreshTCPQualitySnapshots", adminRefreshTCPQualitySnapshots, "Rebuild TCP quality snapshots")
}

func regAdminTCPQuality(name string, handler rpc.Handler, summary string) {
	RegisterWithGroupAndMeta(name, rpc.RoleAdmin, handler, &rpc.MethodMeta{
		Name:    "admin:" + name,
		Summary: summary,
	})
}

func adminGetTCPQualityCatalog(ctx context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	catalog, err := utils.GetTCPQualityCatalog(ctx, false)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return catalog, nil
}

func adminRefreshTCPQualityCatalog(ctx context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	catalog, err := utils.GetTCPQualityCatalog(ctx, true)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return catalog, nil
}

func adminGetTCPQualityTasks(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	taskList, err := tasks.GetAllTCPQualityTasks()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return taskList, nil
}

func adminAddTCPQualityTask(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var task models.TCPQualityTask
	if err := req.BindParams(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	id, err := tasks.AddTCPQualityTask(&task)
	if err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"task_id": id}, nil
}

func adminEditTCPQualityTask(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var task models.TCPQualityTask
	if err := req.BindParams(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	if err := tasks.EditTCPQualityTask(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}

func adminDeleteTCPQualityTasks(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		ID []uint `json:"id"`
	}
	if err := req.BindParams(&params); err != nil || len(params.ID) == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "id is required", nil)
	}
	if err := tasks.DeleteTCPQualityTasks(params.ID); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}

func adminRefreshTCPQualitySnapshots(ctx context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	if err := tasks.RefreshTCPQualitySnapshots(ctx); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}

func adminRunTCPQualityTaskNow(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		ID uint `json:"id"`
	}
	if err := req.BindParams(&params); err != nil || params.ID == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "id is required", nil)
	}
	if err := tasks.RunTCPQualityTaskNow(ctx, params.ID); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"status": "dispatched"}, nil
}

func parseTCPQualityWindow(value string) (int, bool) {
	hours, err := strconv.Atoi(value)
	if err != nil {
		return 0, false
	}
	switch hours {
	case 1, 6, 12, 24, 72, 168:
		return hours, true
	default:
		return 0, false
	}
}
