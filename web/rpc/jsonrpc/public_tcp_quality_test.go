package jsonrpc

import (
	"testing"

	"github.com/komari-monitor/komari/pkg/rpc"
)

func TestTCPQualityTaskIDAcceptsRPCNumberAndRESTString(t *testing.T) {
	for _, test := range []struct {
		name  string
		value any
	}{
		{name: "JSON-RPC number", value: float64(7)},
		{name: "REST query string", value: "7"},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := &rpc.JsonRpcRequest{Params: map[string]any{"task_id": test.value}}
			var params struct {
				TaskID tcpQualityTaskID `json:"task_id"`
			}
			if err := req.BindParams(&params); err != nil {
				t.Fatalf("BindParams() error = %v", err)
			}
			if params.TaskID != 7 {
				t.Fatalf("task_id = %d, want 7", params.TaskID)
			}
		})
	}
}

func TestTCPQualityTaskIDRejectsNonNumericString(t *testing.T) {
	req := &rpc.JsonRpcRequest{Params: map[string]any{"task_id": "not-a-number"}}
	var params struct {
		TaskID tcpQualityTaskID `json:"task_id"`
	}
	if err := req.BindParams(&params); err == nil {
		t.Fatal("BindParams() succeeded for a non-numeric task_id")
	}
}
