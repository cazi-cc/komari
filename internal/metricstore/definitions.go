package metricstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/komari-monitor/komari/pkg/metric"
)

const defaultBuiltinMetricRetentionDays = 1

// RetentionSummary is the compatibility view of all persisted metric policies.
type RetentionSummary struct {
	AllPositive bool
	MaxDays     int
}

// GetRetentionSummary aggregates the active store's metric definitions. An
// empty definition set is not considered record-enabled.
func GetRetentionSummary(ctx context.Context) (RetentionSummary, error) {
	s := GetStore()
	if s == nil {
		return RetentionSummary{}, fmt.Errorf("metric store not initialized")
	}
	defs, err := s.ListMetrics(ctx)
	if err != nil {
		return RetentionSummary{}, err
	}
	return summarizeRetentionDefinitions(defs), nil
}

func summarizeRetentionDefinitions(defs []metric.Definition) RetentionSummary {
	if len(defs) == 0 {
		return RetentionSummary{}
	}
	summary := RetentionSummary{AllPositive: true}
	for _, def := range defs {
		if def.RetentionDays <= 0 {
			summary.AllPositive = false
		}
		if def.RetentionDays > summary.MaxDays {
			summary.MaxDays = def.RetentionDays
		}
	}
	return summary
}

// createMetricDefinitions creates built-in definitions with explicit policies.
func createMetricDefinitions(ctx context.Context, s *metric.Store) error {
	return createMetricDefinitionsWithDefaultRetention(ctx, s, defaultBuiltinMetricRetentionDays)
}

// EnsureBuiltinMetricDefinitions registers definitions for the server's
// built-in report and ping writers before a standalone Store receives points.
func EnsureBuiltinMetricDefinitions(ctx context.Context, s *metric.Store) error {
	return createMetricDefinitions(ctx, s)
}

func createMetricDefinitionsWithDefaultRetention(ctx context.Context, s *metric.Store, defaultRetentionDays int) error {
	if defaultRetentionDays < defaultBuiltinMetricRetentionDays {
		defaultRetentionDays = defaultBuiltinMetricRetentionDays
	}
	definitions := []metric.Definition{
		{Name: MetricCPU, Type: metric.TypeGauge, Unit: "%", Description: "CPU usage percentage", RetentionDays: defaultRetentionDays},
		{Name: MetricGPU, Type: metric.TypeGauge, Unit: "%", Description: "GPU usage percentage", RetentionDays: defaultRetentionDays},
		{Name: MetricGPUDeviceUsage, Type: metric.TypeGauge, Unit: "%", Description: "Per-device GPU utilization", RetentionDays: defaultRetentionDays},
		{Name: MetricGPUMem, Type: metric.TypeGauge, Unit: "bytes", Description: "GPU memory used", RetentionDays: defaultRetentionDays},
		{Name: MetricGPUMemTotal, Type: metric.TypeGauge, Unit: "bytes", Description: "GPU memory total", RetentionDays: defaultRetentionDays},
		{Name: MetricGPUTemp, Type: metric.TypeGauge, Unit: "°C", Description: "GPU temperature", RetentionDays: defaultRetentionDays},
		{Name: MetricRAM, Type: metric.TypeGauge, Unit: "bytes", Description: "RAM used", RetentionDays: defaultRetentionDays},
		{Name: MetricSwap, Type: metric.TypeGauge, Unit: "bytes", Description: "Swap used", RetentionDays: defaultRetentionDays},
		{Name: MetricLoad, Type: metric.TypeGauge, Unit: "", Description: "System load average", RetentionDays: defaultRetentionDays},
		{Name: MetricDisk, Type: metric.TypeGauge, Unit: "bytes", Description: "Disk used", RetentionDays: defaultRetentionDays},
		{Name: MetricNetIn, Type: metric.TypeGauge, Unit: "bytes/s", Description: "Network in rate", RetentionDays: defaultRetentionDays},
		{Name: MetricNetOut, Type: metric.TypeGauge, Unit: "bytes/s", Description: "Network out rate", RetentionDays: defaultRetentionDays},
		{Name: MetricNetTotalUp, Type: metric.TypeCounter, Unit: "bytes", Description: "Network total upload", RetentionDays: defaultRetentionDays},
		{Name: MetricNetTotalDown, Type: metric.TypeCounter, Unit: "bytes", Description: "Network total download", RetentionDays: defaultRetentionDays},
		{Name: MetricTrafficUp, Type: metric.TypeGauge, Unit: "bytes", Description: "Traffic upload delta", RetentionDays: defaultRetentionDays},
		{Name: MetricTrafficDown, Type: metric.TypeGauge, Unit: "bytes", Description: "Traffic download delta", RetentionDays: defaultRetentionDays},
		{Name: MetricProcess, Type: metric.TypeGauge, Unit: "count", Description: "Process count", RetentionDays: defaultRetentionDays},
		{Name: MetricConnections, Type: metric.TypeGauge, Unit: "count", Description: "TCP connections", RetentionDays: defaultRetentionDays},
		{Name: MetricConnectionsUDP, Type: metric.TypeGauge, Unit: "count", Description: "UDP connections", RetentionDays: defaultRetentionDays},
		{Name: MetricPingLatency, Type: metric.TypeGauge, Unit: "ms", Description: "Ping latency", RetentionDays: defaultRetentionDays},
		{Name: MetricPingLoss, Type: metric.TypeGauge, Unit: "ratio", Description: "Ping packet loss indicator", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeReachable, Type: metric.TypeGauge, Unit: "bool", Description: "Probe transport reachability", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeLatencyMin, Type: metric.TypeGauge, Unit: "ms", Description: "Minimum latency within a probe run", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeLatencyMax, Type: metric.TypeGauge, Unit: "ms", Description: "Maximum latency within a probe run", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeJitter, Type: metric.TypeGauge, Unit: "ms", Description: "Latency standard deviation within a probe run", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeSamplesSent, Type: metric.TypeGauge, Unit: "count", Description: "Samples attempted within a probe run", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeSamplesReceived, Type: metric.TypeGauge, Unit: "count", Description: "Samples receiving a network response", RetentionDays: defaultRetentionDays},
		{Name: MetricProbePacketSize, Type: metric.TypeGauge, Unit: "bytes", Description: "ICMP payload size", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeDNS, Type: metric.TypeGauge, Unit: "ms", Description: "DNS resolution duration", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeConnect, Type: metric.TypeGauge, Unit: "ms", Description: "TCP connection duration", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeTLS, Type: metric.TypeGauge, Unit: "ms", Description: "TLS handshake duration", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeTTFB, Type: metric.TypeGauge, Unit: "ms", Description: "HTTP time to first byte", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeHTTPStatus, Type: metric.TypeGauge, Unit: "code", Description: "Last HTTP status code in a probe run", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeHTTPStatusOK, Type: metric.TypeGauge, Unit: "ratio", Description: "Share of HTTP responses matching the configured status policy", RetentionDays: defaultRetentionDays},
		{Name: MetricProbeTCPRetrans, Type: metric.TypeGauge, Unit: "count", Description: "Connection-level TCP retransmissions observed through TCP_INFO", RetentionDays: defaultRetentionDays},
	}

	for _, def := range definitions {
		existing, err := s.GetMetric(ctx, def.Name)
		if err != nil && !errors.Is(err, metric.ErrNotFound) {
			return fmt.Errorf("failed to get metric %s: %w", def.Name, err)
		}
		if err == nil {
			if existing.RetentionDays == 0 {
				continue
			}
			def.RetentionDays = existing.RetentionDays
		}
		if err := s.UpsertMetric(ctx, def); err != nil {
			return fmt.Errorf("failed to create metric %s: %w", def.Name, err)
		}
	}
	return nil
}
