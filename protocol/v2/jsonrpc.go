package v2

import (
	"time"

	v1 "github.com/komari-monitor/komari/protocol/v1"
)

const (
	Version                        = "2.0"
	MethodAgentReport              = "agent.report"
	MethodAgentBasicInfo           = "agent.basicInfo"
	MethodAgentPingResult          = "agent.pingResult"
	MethodAgentTaskResult          = "agent.taskResult"
	MethodAgentExec                = "agent.exec"
	MethodAgentPing                = "agent.ping"
	MethodAgentTCPQuality          = "agent.tcpQuality"
	MethodAgentTCPQualityResult    = "agent.tcpQualityResult"
	MethodAgentUnlockQuality       = "agent.unlockQuality"
	MethodAgentUnlockQualityResult = "agent.unlockQualityResult"
	MethodAgentMessage             = "agent.message"
	MethodAgentEvent               = "agent.event"
	MethodAgentTerminal            = "agent.terminal.request"
	MethodAgentPull                = "agent.pull"
)

type Request struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
	ID      any    `json:"id,omitempty"`
}

type Response struct {
	JSONRPC string    `json:"jsonrpc"`
	ID      any       `json:"id,omitempty"`
	Result  any       `json:"result,omitempty"`
	Error   *RPCError `json:"error,omitempty"`
}

type Event struct {
	ID        string    `json:"id"`
	Method    string    `json:"method"`
	Params    any       `json:"params,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	ExpiresAt time.Time `json:"expires_at"`
}

type RPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type ReportParams struct {
	Report      v1.Report `json:"report"`
	AckEventIDs []string  `json:"ack_event_ids,omitempty"`
}

type BasicInfoParams struct {
	Info map[string]interface{} `json:"info"`
}

type PingResultParams struct {
	TaskID     uint                `json:"task_id"`
	PingType   string              `json:"ping_type"`
	Value      int                 `json:"value"`
	Details    *ProbeResultDetails `json:"details,omitempty"`
	FinishedAt time.Time           `json:"finished_at"`
}

type PullParams struct {
	Capabilities []string `json:"capabilities,omitempty"`
	AckEventIDs  []string `json:"ack_event_ids,omitempty"`
	LastEventID  string   `json:"last_event_id,omitempty"`
}

type ExecParams struct {
	TaskID  string `json:"task_id"`
	Command string `json:"command"`
}

type PingParams struct {
	TaskID  uint         `json:"ping_task_id"`
	Type    string       `json:"ping_type"`
	Target  string       `json:"ping_target"`
	Options ProbeOptions `json:"ping_options,omitempty"`
}

type TCPQualityTarget struct {
	Key          string `json:"key"`
	Address      string `json:"address"`
	Port         int    `json:"port"`
	Province     string `json:"province"`
	ProvinceCode string `json:"province_code"`
	ISP          string `json:"isp"`
	ISPCode      string `json:"isp_code"`
	IPVersion    int    `json:"ip_version"`
}

type TCPQualityParams struct {
	TaskID          uint               `json:"task_id"`
	RunID           string             `json:"run_id"`
	CatalogRevision string             `json:"catalog_revision"`
	Targets         []TCPQualityTarget `json:"targets"`
	StandardPackets int                `json:"standard_packets"`
	LargeEnabled    bool               `json:"large_enabled"`
	LargePackets    int                `json:"large_packets"`
	DelayMS         int                `json:"delay_ms"`
	TimeoutMS       int                `json:"timeout_ms"`
	MaxParallel     int                `json:"max_parallel"`
}

type TCPQualityTargetResult struct {
	TargetKey        string  `json:"target_key"`
	Mode             string  `json:"mode"`
	SamplesSent      int     `json:"samples_sent"`
	SamplesReceived  int     `json:"samples_received"`
	LossRatio        float64 `json:"loss_ratio"`
	MinLatencyMS     float64 `json:"min_latency_ms,omitempty"`
	MaxLatencyMS     float64 `json:"max_latency_ms,omitempty"`
	P50LatencyMS     float64 `json:"p50_latency_ms,omitempty"`
	P95LatencyMS     float64 `json:"p95_latency_ms,omitempty"`
	AverageLatencyMS float64 `json:"average_latency_ms,omitempty"`
	ErrorCode        string  `json:"error_code,omitempty"`
}

type TCPQualityResultParams struct {
	TaskID          uint                     `json:"task_id"`
	RunID           string                   `json:"run_id"`
	CatalogRevision string                   `json:"catalog_revision"`
	Results         []TCPQualityTargetResult `json:"results"`
	ErrorCode       string                   `json:"error_code,omitempty"`
	FinishedAt      time.Time                `json:"finished_at"`
}

type UnlockQualityParams struct {
	TaskID          uint   `json:"task_id"`
	RunID           string `json:"run_id"`
	Service         string `json:"service"`
	CatalogRevision string `json:"catalog_revision"`
	RouteMode       string `json:"route_mode"`
	ProbeKind       string `json:"probe_kind"`
	DNSServer       string `json:"dns_server,omitempty"`
	FixedAddress    string `json:"fixed_address,omitempty"`
	SampleCount     int    `json:"sample_count"`
	TimeoutMS       int    `json:"timeout_ms"`
}

type UnlockQualityEndpointResult struct {
	EndpointKey           string  `json:"endpoint_key"`
	SamplesSent           int     `json:"samples_sent"`
	SamplesReceived       int     `json:"samples_received"`
	FailureRatio          float64 `json:"failure_ratio"`
	DNSMS                 float64 `json:"dns_ms,omitempty"`
	ConnectMS             float64 `json:"connect_ms,omitempty"`
	TLSMS                 float64 `json:"tls_ms,omitempty"`
	TTFBP50MS             float64 `json:"ttfb_p50_ms,omitempty"`
	TTFBP95MS             float64 `json:"ttfb_p95_ms,omitempty"`
	TotalP50MS            float64 `json:"total_p50_ms,omitempty"`
	TotalP95MS            float64 `json:"total_p95_ms,omitempty"`
	JitterMS              float64 `json:"jitter_ms,omitempty"`
	HTTPStatusCode        int     `json:"http_status_code,omitempty"`
	HTTPStatusOKRatio     float64 `json:"http_status_ok_ratio,omitempty"`
	TCPRetransmissions    int     `json:"tcp_retransmissions,omitempty"`
	ResolvedAddressHash   string  `json:"resolved_address_hash,omitempty"`
	ResolvedAddressFamily string  `json:"resolved_address_family,omitempty"`
	ExitCountry           string  `json:"exit_country,omitempty"`
	EdgeColo              string  `json:"edge_colo,omitempty"`
	Verdict               string  `json:"verdict,omitempty"`
	ErrorCode             string  `json:"error_code,omitempty"`
}

type UnlockQualityResultParams struct {
	TaskID          uint                          `json:"task_id"`
	RunID           string                        `json:"run_id"`
	Service         string                        `json:"service"`
	CatalogRevision string                        `json:"catalog_revision"`
	RouteMode       string                        `json:"route_mode"`
	ProbeKind       string                        `json:"probe_kind"`
	Verdict         string                        `json:"verdict"`
	Results         []UnlockQualityEndpointResult `json:"results"`
	ErrorCode       string                        `json:"error_code,omitempty"`
	FinishedAt      time.Time                     `json:"finished_at"`
}

type ProbeOptions struct {
	PacketSize       int    `json:"packet_size,omitempty"`
	SampleCount      int    `json:"sample_count,omitempty"`
	TimeoutMS        int    `json:"timeout_ms,omitempty"`
	DNSServer        string `json:"dns_server,omitempty"`
	PreferredIP      string `json:"preferred_ip,omitempty"`
	ValidStatusCodes []int  `json:"valid_status_codes,omitempty"`
}

type ProbeResultDetails struct {
	Reachable             bool    `json:"reachable"`
	SamplesSent           int     `json:"samples_sent,omitempty"`
	SamplesReceived       int     `json:"samples_received,omitempty"`
	LossRatio             float64 `json:"loss_ratio,omitempty"`
	PacketSize            int     `json:"packet_size,omitempty"`
	MinLatencyMS          float64 `json:"min_latency_ms,omitempty"`
	MaxLatencyMS          float64 `json:"max_latency_ms,omitempty"`
	AverageLatencyMS      float64 `json:"average_latency_ms,omitempty"`
	JitterMS              float64 `json:"jitter_ms,omitempty"`
	DNSMS                 float64 `json:"dns_ms,omitempty"`
	ConnectMS             float64 `json:"connect_ms,omitempty"`
	TLSMS                 float64 `json:"tls_ms,omitempty"`
	TTFBMS                float64 `json:"ttfb_ms,omitempty"`
	HTTPStatusCode        int     `json:"http_status_code,omitempty"`
	HTTPStatusOKRatio     float64 `json:"http_status_ok_ratio,omitempty"`
	TCPRetransmissions    int     `json:"tcp_retransmissions,omitempty"`
	ResolvedAddressHash   string  `json:"resolved_address_hash,omitempty"`
	ResolvedAddressFamily string  `json:"resolved_address_family,omitempty"`
	DNSMode               string  `json:"dns_mode,omitempty"`
	ErrorCode             string  `json:"error_code,omitempty"`
}

type MessageParams struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

type EventParams struct {
	Type string `json:"type"`
	Data any    `json:"data,omitempty"`
}

type TerminalRequestParams struct {
	RequestID string `json:"request_id"`
}

func Success(id any, result any) Response {
	return Response{JSONRPC: Version, ID: id, Result: result}
}

func Error(id any, code int, message string, data any) Response {
	return Response{JSONRPC: Version, ID: id, Error: &RPCError{Code: code, Message: message, Data: data}}
}
