package models

import (
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"time"
)

type PingRecord struct {
	Client     string              `json:"client" gorm:"type:varchar(36);not null;index"`
	ClientInfo Client              `json:"client_info" gorm:"foreignKey:Client;references:UUID;constraint:OnDelete:CASCADE,OnUpdate:CASCADE"`
	TaskId     uint                `json:"task_id" gorm:"not null;index"`
	Task       PingTask            `json:"task" gorm:"foreignKey:TaskId;references:Id;constraint:OnDelete:CASCADE,OnUpdate:CASCADE;"`
	Time       time.Time           `json:"time" gorm:"index;not null"`
	Value      int                 `json:"value" gorm:"type:int;not null"` // Ping 值，单位毫秒
	PingType   string              `json:"ping_type,omitempty" gorm:"-"`
	Details    *ProbeResultDetails `json:"details,omitempty" gorm:"-"`
}

type ProbeConfig struct {
	PacketSize       int    `json:"packet_size,omitempty"`
	SampleCount      int    `json:"sample_count,omitempty"`
	TimeoutMS        int    `json:"timeout_ms,omitempty"`
	DNSServer        string `json:"dns_server,omitempty"`
	PreferredIP      string `json:"preferred_ip,omitempty"`
	ValidStatusCodes []int  `json:"valid_status_codes,omitempty"`
}

func (config *ProbeConfig) Scan(value any) error {
	if value == nil {
		*config = ProbeConfig{}
		return nil
	}
	var data []byte
	switch typed := value.(type) {
	case []byte:
		data = typed
	case string:
		data = []byte(typed)
	default:
		return fmt.Errorf("failed to scan ProbeConfig: unsupported value type %T", value)
	}
	if len(data) == 0 {
		*config = ProbeConfig{}
		return nil
	}
	return json.Unmarshal(data, config)
}

func (config ProbeConfig) Value() (driver.Value, error) {
	data, err := json.Marshal(config)
	if err != nil {
		return nil, err
	}
	return string(data), nil
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

// PingTask 表示一次延迟监测任务配置。
type PingTask struct {
	Id          uint        `json:"id,omitempty" gorm:"primaryKey;autoIncrement"`
	Weight      int         `json:"weight" gorm:"type:int;not null;default:0;index"`
	Name        string      `json:"name" gorm:"type:varchar(255);not null;index"`
	Clients     StringArray `json:"clients" gorm:"type:longtext"`
	DefaultOn   bool        `json:"default_on" gorm:"column:all_clients;not null;default:false"` // 新加入的服务器是否自动开启此监测；现有服务器不受此字段影响
	Type        string      `json:"type" gorm:"type:varchar(12);not null;default:'icmp'"`        // icmp tcp http
	Target      string      `json:"target" gorm:"type:varchar(255);not null"`                    // Ping 目标地址
	Interval    int         `json:"interval" gorm:"type:int;not null;default:60"`                // 间隔时间
	ProbeConfig ProbeConfig `json:"probe_config,omitempty" gorm:"type:longtext;not null;default:'{}'"`
}

// AppliesToClient 判断当前 PingTask 是否适用于指定服务器。
func (task PingTask) AppliesToClient(uuid string) bool {
	if uuid == "" {
		return false
	}
	for _, client := range task.Clients {
		if client == uuid {
			return true
		}
	}
	return false
}
