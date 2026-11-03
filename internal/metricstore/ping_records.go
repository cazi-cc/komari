package metricstore

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/pkg/metric"
)

// WritePingRecord 将 ping 记录写入 metric store
func WritePingRecord(ctx context.Context, rec models.PingRecord) error {
	s := GetStore()
	if s == nil {
		return fmt.Errorf("metric store not enabled")
	}

	reportBatcherMu.Lock()
	worker := reportBatcher
	reportBatcherMu.Unlock()
	if worker != nil {
		return worker.enqueuePing(ctx, rec)
	}
	return writePingRecords(ctx, []models.PingRecord{rec})
}

func writePingRecords(ctx context.Context, records []models.PingRecord) error {
	s := GetStore()
	if s == nil {
		return fmt.Errorf("metric store not enabled")
	}
	if len(records) == 0 {
		return nil
	}

	points := make([]metric.Point, 0, len(records)*10)
	for _, rec := range records {
		tags := map[string]string{"task_id": fmt.Sprintf("%d", rec.TaskId)}
		loss := 0.0
		if rec.Value < 0 {
			loss = 1
		}
		if rec.Details != nil {
			loss = clampRatio(rec.Details.LossRatio)
		}
		labels := probeMetricLabels(rec.Details)
		points = append(points,
			metric.Point{
				MetricName: MetricPingLatency,
				EntityID:   rec.Client,
				Timestamp:  rec.Time,
				Value:      float64(rec.Value),
				Tags:       tags,
				Labels:     labels,
			},
			metric.Point{
				MetricName: MetricPingLoss,
				EntityID:   rec.Client,
				Timestamp:  rec.Time,
				Value:      loss,
				Tags:       tags,
				Labels:     labels,
			},
		)
		if rec.Details == nil {
			continue
		}
		reachable := 0.0
		if rec.Details.Reachable {
			reachable = 1
		}
		points = append(points,
			probeMetricPoint(MetricProbeReachable, rec, tags, labels, reachable),
			probeMetricPoint(MetricProbeSamplesSent, rec, tags, labels, float64(rec.Details.SamplesSent)),
			probeMetricPoint(MetricProbeSamplesReceived, rec, tags, labels, float64(rec.Details.SamplesReceived)),
		)
		if rec.Details.SamplesReceived > 0 {
			points = append(points,
				probeMetricPoint(MetricProbeLatencyMin, rec, tags, labels, rec.Details.MinLatencyMS),
				probeMetricPoint(MetricProbeLatencyMax, rec, tags, labels, rec.Details.MaxLatencyMS),
				probeMetricPoint(MetricProbeJitter, rec, tags, labels, rec.Details.JitterMS),
			)
		}
		if rec.Details.PacketSize > 0 {
			points = append(points, probeMetricPoint(MetricProbePacketSize, rec, tags, labels, float64(rec.Details.PacketSize)))
		}
		if rec.Details.DNSMS > 0 {
			points = append(points, probeMetricPoint(MetricProbeDNS, rec, tags, labels, rec.Details.DNSMS))
		}
		if rec.Details.ConnectMS > 0 {
			points = append(points, probeMetricPoint(MetricProbeConnect, rec, tags, labels, rec.Details.ConnectMS))
		}
		if rec.Details.TLSMS > 0 {
			points = append(points, probeMetricPoint(MetricProbeTLS, rec, tags, labels, rec.Details.TLSMS))
		}
		if rec.Details.TTFBMS > 0 {
			points = append(points, probeMetricPoint(MetricProbeTTFB, rec, tags, labels, rec.Details.TTFBMS))
		}
		if rec.PingType == "http" || rec.Details.HTTPStatusCode > 0 {
			if rec.Details.HTTPStatusCode > 0 {
				points = append(points, probeMetricPoint(MetricProbeHTTPStatus, rec, tags, labels, float64(rec.Details.HTTPStatusCode)))
			}
			points = append(points, probeMetricPoint(MetricProbeHTTPStatusOK, rec, tags, labels, clampRatio(rec.Details.HTTPStatusOKRatio)))
		}
		if rec.PingType == "tcp" || rec.PingType == "http" {
			points = append(points, probeMetricPoint(MetricProbeTCPRetrans, rec, tags, labels, float64(rec.Details.TCPRetransmissions)))
		}
	}
	return s.WriteBatch(ctx, points)
}
func GetPingRecords(ctx context.Context, clientUUID string, taskID int, start, end time.Time) ([]models.PingRecord, error) {
	s := GetStore()
	if s == nil {
		return nil, fmt.Errorf("metric store not enabled")
	}

	query := MetricsQuery{
		Store: s, Mode: QuerySeries, Source: SourceRollup,
		CompactDefinitions: true,
		Entities:           EntityScope{All: clientUUID == ""},
		Specs:              []MetricSpec{{Name: MetricPingLatency, Aggregations: []metric.Aggregation{metric.AggLast}}},
		Start:              start, End: end, Order: metric.OrderAsc, Grouping: GroupSeries,
		Budget: PointBudget{PerSeries: 4000}, Rounding: RoundFloor, AllowMissing: true,
	}
	if clientUUID != "" {
		query.Entities.IDs = []string{clientUUID}
	}
	if taskID >= 0 {
		query.Tags = map[string]string{"task_id": fmt.Sprintf("%d", taskID)}
	}
	loaded, err := QueryMetrics(ctx, query)
	if err != nil {
		return nil, err
	}

	points := loaded.Values[MetricPingLatency][metric.AggLast]
	records := make([]models.PingRecord, 0, len(points))
	for _, p := range points {
		taskIDVal := uint(0)
		if tid, ok := p.Tags["task_id"]; ok {
			var t uint64
			fmt.Sscanf(tid, "%d", &t)
			taskIDVal = uint(t)
		}

		records = append(records, models.PingRecord{
			Client: p.EntityID,
			TaskId: taskIDVal,
			Time:   p.Bucket.UTC(),
			Value:  int(p.Value),
		})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Time.After(records[j].Time)
	})

	return records, nil
}

func clampRatio(value float64) float64 {
	if value < 0 {
		return 0
	}
	if value > 1 {
		return 1
	}
	return value
}

func probeMetricLabels(details *models.ProbeResultDetails) map[string]string {
	if details == nil {
		return nil
	}
	labels := map[string]string{}
	if details.ResolvedAddressFamily != "" {
		labels["address_family"] = details.ResolvedAddressFamily
	}
	if details.DNSMode != "" {
		labels["dns_mode"] = details.DNSMode
	}
	if details.ErrorCode != "" {
		labels["error_code"] = details.ErrorCode
	}
	if len(labels) == 0 {
		return nil
	}
	return labels
}

func probeMetricPoint(name string, rec models.PingRecord, tags, labels map[string]string, value float64) metric.Point {
	return metric.Point{
		MetricName: name,
		EntityID:   rec.Client,
		Timestamp:  rec.Time,
		Value:      value,
		Tags:       tags,
		Labels:     labels,
	}
}

// GetRecordsByClientAndTime 从 metric store 查询记录并重构为 models.Record
