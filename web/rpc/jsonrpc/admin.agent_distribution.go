package jsonrpc

import (
	"context"

	"github.com/komari-monitor/komari/internal/agentdist"
	"github.com/komari-monitor/komari/pkg/rpc"
)

func init() {
	RegisterWithGroupAndMeta("getAgentDistribution", rpc.RoleAdmin, adminGetAgentDistribution, &rpc.MethodMeta{
		Name:    "admin:getAgentDistribution",
		Summary: "Get the backend-controlled agent installer distribution",
		Returns: "AgentDistribution",
	})
}

func adminGetAgentDistribution(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	return agentdist.Current(), nil
}
