package jsonrpc

import (
	"context"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/pkg/rpc"
)

func init() {
	regAdminUnlockQuality("getUnlockQualityTasks", adminGetUnlockQualityTasks, "List unlock quality tasks")
	regAdminUnlockQuality("addUnlockQualityTask", adminAddUnlockQualityTask, "Create an unlock quality task")
	regAdminUnlockQuality("editUnlockQualityTask", adminEditUnlockQualityTask, "Edit an unlock quality task")
	regAdminUnlockQuality("deleteUnlockQualityTasks", adminDeleteUnlockQualityTasks, "Delete unlock quality tasks")
	regAdminUnlockQuality("runUnlockQualityTaskNow", adminRunUnlockQualityTaskNow, "Dispatch a full unlock verification now")
	regAdminUnlockQuality("refreshUnlockQualitySnapshots", adminRefreshUnlockQualitySnapshots, "Rebuild unlock quality snapshots")
}

func regAdminUnlockQuality(name string, handler rpc.Handler, summary string) {
	RegisterWithGroupAndMeta(name, rpc.RoleAdmin, handler, &rpc.MethodMeta{
		Name: "admin:" + name, Summary: summary,
	})
}

func adminGetUnlockQualityTasks(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	result, err := tasks.GetAllUnlockQualityTasks()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return result, nil
}

func adminAddUnlockQualityTask(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var task models.UnlockQualityTask
	if err := req.BindParams(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	id, err := tasks.AddUnlockQualityTask(&task)
	if err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"task_id": id}, nil
}

func adminEditUnlockQualityTask(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var task models.UnlockQualityTask
	if err := req.BindParams(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	if err := tasks.EditUnlockQualityTask(&task); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}

func adminDeleteUnlockQualityTasks(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		ID []uint `json:"id"`
	}
	if err := req.BindParams(&params); err != nil || len(params.ID) == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "id is required", nil)
	}
	if err := tasks.DeleteUnlockQualityTasks(params.ID); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}

func adminRunUnlockQualityTaskNow(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		ID uint `json:"id"`
	}
	if err := req.BindParams(&params); err != nil || params.ID == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "id is required", nil)
	}
	if err := tasks.RunUnlockQualityTaskNow(ctx, params.ID); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	return map[string]any{"status": "dispatched"}, nil
}

func adminRefreshUnlockQualitySnapshots(ctx context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	if err := tasks.RefreshUnlockQualitySnapshots(ctx, true); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, err.Error(), nil)
	}
	return map[string]any{"status": "success"}, nil
}
