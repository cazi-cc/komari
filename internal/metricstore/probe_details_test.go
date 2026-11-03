package metricstore

import (
	"context"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/pkg/metric"
	"testing"
	"time"
)

func TestDetailedProbeWritesPartialLossHTTPPhasesAndRetransmissions(t *testing.T) {
	ctx := context.Background()
	s := useReportTestStore(t, nil)
	base := time.Now().UTC().Truncate(time.Second)

	record := models.PingRecord{
		Client:   "http-node",
		TaskId:   11,
		Time:     base,
		Value:    72,
		PingType: "http",
		Details: &models.ProbeResultDetails{
			Reachable:             true,
			SamplesSent:           5,
			SamplesReceived:       3,
			LossRatio:             0.4,
			MinLatencyMS:          60,
			MaxLatencyMS:          91,
			AverageLatencyMS:      72,
			JitterMS:              12,
			DNSMS:                 4,
			ConnectMS:             18,
			TLSMS:                 25,
			TTFBMS:                68,
			HTTPStatusCode:        403,
			HTTPStatusOKRatio:     0,
			TCPRetransmissions:    2,
			ResolvedAddressHash:   "a1b2c3d4e5f6",
			ResolvedAddressFamily: "ipv4",
			DNSMode:               "custom",
		},
	}
	if err := WritePingRecord(ctx, record); err != nil {
		t.Fatalf("write detailed ping record: %v", err)
	}

	assertProbeMetric := func(name string, want float64) metric.Point {
		t.Helper()
		points, err := s.Query(ctx, metric.Query{
			MetricName: name,
			EntityID:   record.Client,
			Tags:       map[string]string{"task_id": "11"},
			Start:      base.Add(-time.Second),
			End:        base.Add(time.Second),
		})
		if err != nil {
			t.Fatalf("query %s: %v", name, err)
		}
		if len(points) != 1 || points[0].Value != want {
			t.Fatalf("%s points = %#v, want one value %v", name, points, want)
		}
		return points[0]
	}

	loss := assertProbeMetric(MetricPingLoss, 0.4)
	if _, exists := loss.Labels["address_hash"]; exists {
		t.Fatalf("loss labels leaked resolved address hash: %#v", loss.Labels)
	}
	if loss.Labels["dns_mode"] != "custom" || loss.Labels["address_family"] != "ipv4" {
		t.Fatalf("loss labels = %#v, want non-sensitive DNS metadata", loss.Labels)
	}
	assertProbeMetric(MetricProbeHTTPStatus, 403)
	assertProbeMetric(MetricProbeHTTPStatusOK, 0)
	assertProbeMetric(MetricProbeTCPRetrans, 2)
	assertProbeMetric(MetricProbeLatencyMin, 60)
	assertProbeMetric(MetricProbeLatencyMax, 91)
	assertProbeMetric(MetricProbeJitter, 12)
}
