package jsonrpc

import (
	"context"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/internal/metricstore"
	"github.com/komari-monitor/komari/pkg/metric"
	"github.com/komari-monitor/komari/pkg/rpc"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

const canonicalPingStatsSamplePoints = 4000

func publicGetPingMetricWindowStats(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params publicPingMetricStatsParams
	if err := req.BindParams(&params); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, "Invalid request body: "+err.Error(), nil)
	}

	end := metricQueryTimeOrDefault(firstMetricQueryTime(params.End, params.EndTime), time.Now().UTC())
	startFallback := end.Add(-metricQueryHours(params.Hours))
	start := metricQueryTimeOrDefault(firstMetricQueryTime(params.Start, params.StartTime), startFallback)
	if !end.After(start) {
		return nil, rpc.MakeError(rpc.InvalidParams, "end must be after start", nil)
	}

	requestedEntities := normalizeStringList(params.EntityIDs, []string{firstNonEmpty(params.EntityID, params.UUID)})
	entityIDs, rpcErr := publicMetricEntityIDs(ctx, requestedEntities)
	if rpcErr != nil {
		return nil, rpcErr
	}
	if len(entityIDs) == 0 {
		return publicPingMetricStatsResponse{
			Start: start.UTC(),
			End:   end.UTC(),
			Stats: []publicPingMetricTaskStats{},
			Count: 0,
		}, nil
	}

	store := metricstore.GetStore()
	if store == nil {
		return nil, rpc.MakeError(rpc.InternalError, "metric store not initialized", nil)
	}

	taskList, err := tasks.GetAllPingTasks()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to fetch ping tasks: "+err.Error(), nil)
	}
	taskMap := make(map[string]models.PingTask, len(taskList))
	for _, task := range taskList {
		taskMap[strconv.FormatUint(uint64(task.Id), 10)] = task
	}
	taskFilter := normalizePingMetricTaskIDs(params.TaskID, params.TaskIDs)

	now := time.Now().UTC()
	interval := metric.FloorStandardInterval(
		time.Duration((end.Sub(start).Nanoseconds() + canonicalPingStatsSamplePoints - 1) / canonicalPingStatsSamplePoints),
	)
	if interval < time.Second {
		interval = time.Second
	}
	interval = store.CompatibleSeriesInterval(start, now, interval)

	records, err := metricstore.GetPingRecords(ctx, singleStringValue(entityIDs), singlePingMetricTaskID(taskFilter), start, end)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to query ping latency snapshots: "+err.Error(), nil)
	}
	lossGroups, err := loadPublicPingLossWindow(ctx, store, entityIDs, taskFilter, start, end, now)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to query ping loss statistics: "+err.Error(), nil)
	}
	stats := publicPingStatsFromSnapshots(records, lossGroups, entityIDs, taskMap, taskFilter)

	sort.Slice(stats, func(i, j int) bool {
		if stats[i].EntityID != stats[j].EntityID {
			return stats[i].EntityID < stats[j].EntityID
		}
		return stats[i].TaskID < stats[j].TaskID
	})

	return publicPingMetricStatsResponse{
		Start:           start.UTC(),
		End:             end.UTC(),
		IntervalSeconds: interval.Seconds(),
		Stats:           stats,
		Count:           len(stats),
	}, nil
}

type publicPingMetricSeriesKey struct {
	EntityID string
	TaskID   string
}

type publicPingLatencySnapshots struct {
	Values     []float64
	Total      int
	Latest     *float64
	LatestTime time.Time
}

func singleStringValue(values []string) string {
	if len(values) == 1 {
		return values[0]
	}
	return ""
}

func singlePingMetricTaskID(taskFilter map[string]bool) int {
	if len(taskFilter) != 1 {
		return -1
	}
	for taskID := range taskFilter {
		value, err := strconv.Atoi(taskID)
		if err == nil {
			return value
		}
	}
	return -1
}

func loadPublicPingLossWindow(
	ctx context.Context,
	store *metric.Store,
	entityIDs []string,
	taskFilter map[string]bool,
	start, end, now time.Time,
) (map[publicPingMetricSeriesKey]metric.AggregatePoint, error) {
	query := metric.Query{
		MetricName: metricstore.MetricPingLoss,
		EntityID:   singleStringValue(entityIDs),
		Start:      start,
		End:        end,
		Order:      metric.OrderAsc,
	}
	if taskID := singlePingMetricTaskID(taskFilter); taskID >= 0 {
		query.Tags = map[string]string{"task_id": strconv.Itoa(taskID)}
	}

	points, err := store.Series(ctx, metric.AggregateQuery{
		Query:          query,
		Aggregation:    metric.AggAvg,
		Interval:       metricWindowAggregateInterval(end),
		PreserveSeries: true,
	}, now)
	if err != nil {
		return nil, err
	}

	allowedEntities := make(map[string]bool, len(entityIDs))
	for _, entityID := range entityIDs {
		allowedEntities[entityID] = true
	}
	out := make(map[publicPingMetricSeriesKey]metric.AggregatePoint)
	for _, point := range points {
		taskID := strings.TrimSpace(point.Tags["task_id"])
		if !allowedEntities[point.EntityID] || taskID == "" {
			continue
		}
		if len(taskFilter) > 0 && !taskFilter[taskID] {
			continue
		}
		out[publicPingMetricSeriesKey{EntityID: point.EntityID, TaskID: taskID}] = point
	}
	return out, nil
}

func publicPingStatsFromSnapshots(
	records []models.PingRecord,
	lossGroups map[publicPingMetricSeriesKey]metric.AggregatePoint,
	entityIDs []string,
	taskMap map[string]models.PingTask,
	taskFilter map[string]bool,
) []publicPingMetricTaskStats {
	allowedEntities := make(map[string]bool, len(entityIDs))
	for _, entityID := range entityIDs {
		allowedEntities[entityID] = true
	}

	latencyGroups := make(map[publicPingMetricSeriesKey]*publicPingLatencySnapshots)
	keys := make(map[publicPingMetricSeriesKey]struct{})
	for _, record := range records {
		taskID := strconv.FormatUint(uint64(record.TaskId), 10)
		key := publicPingMetricSeriesKey{EntityID: record.Client, TaskID: taskID}
		if !allowedEntities[record.Client] || (len(taskFilter) > 0 && !taskFilter[taskID]) {
			continue
		}
		group := latencyGroups[key]
		if group == nil {
			group = &publicPingLatencySnapshots{}
			latencyGroups[key] = group
		}
		group.Total++
		keys[key] = struct{}{}
		if record.Value < 0 {
			continue
		}
		value := float64(record.Value)
		group.Values = append(group.Values, value)
		if group.Latest == nil || record.Time.After(group.LatestTime) {
			latest := value
			group.Latest = &latest
			group.LatestTime = record.Time
		}
	}
	for key := range lossGroups {
		keys[key] = struct{}{}
	}

	out := make([]publicPingMetricTaskStats, 0, len(keys))
	for key := range keys {
		group := latencyGroups[key]
		if group == nil {
			group = &publicPingLatencySnapshots{}
		}
		lossPoint, hasLoss := lossGroups[key]
		total := group.Total
		lossRate := 0.0
		approximate := !hasLoss
		valid := len(group.Values)
		if hasLoss {
			total = lossPoint.Count
			lossFraction := math.Max(0, math.Min(1, lossPoint.Value))
			lossRate = lossFraction * 100
			valid = total - int(math.Round(lossFraction*float64(total)))
			if valid < 0 {
				valid = 0
			}
		} else if total > 0 {
			lossRate = float64(total-valid) / float64(total) * 100
		}
		if total == 0 {
			continue
		}

		stat := publicPingMetricTaskStats{
			EntityID:        key.EntityID,
			TaskID:          key.TaskID,
			Tags:            map[string]string{"task_id": key.TaskID},
			Total:           total,
			Valid:           valid,
			Loss:            lossRate,
			LossApproximate: approximate,
			Latest:          group.Latest,
		}
		if task, ok := taskMap[key.TaskID]; ok {
			stat.Name = task.Name
			stat.Type = task.Type
			stat.Interval = task.Interval
		}
		if len(group.Values) > 0 {
			sort.Float64s(group.Values)
			stat.Min = float64Pointer(group.Values[0])
			stat.Max = float64Pointer(group.Values[len(group.Values)-1])
			stat.Avg = float64Pointer(averageFloat64(group.Values))
			stat.P005 = float64Pointer(percentileFloat64(group.Values, 0.005))
			stat.P50 = float64Pointer(percentileFloat64(group.Values, 0.50))
			stat.P95 = float64Pointer(percentileFloat64(group.Values, 0.95))
			stat.P99 = float64Pointer(percentileFloat64(group.Values, 0.99))
			stat.P995 = float64Pointer(percentileFloat64(group.Values, 0.995))
			stat.StdDev = float64Pointer(populationStdDev(group.Values))
		}
		if stat.P50 != nil && stat.P99 != nil && *stat.P50 > 0 && *stat.P99 >= *stat.P50 {
			adjustedBase := math.Max(math.Min(*stat.P50, 50.0), 10.0)
			stat.P99P50Ratio = (*stat.P99 - *stat.P50) / adjustedBase
		}
		out = append(out, stat)
	}
	return out
}

func float64Pointer(value float64) *float64 {
	return &value
}

func averageFloat64(values []float64) float64 {
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func percentileFloat64(sortedValues []float64, percentile float64) float64 {
	position := float64(len(sortedValues)-1) * math.Max(0, math.Min(1, percentile))
	lower := int(math.Floor(position))
	upper := int(math.Ceil(position))
	if lower == upper {
		return sortedValues[lower]
	}
	fraction := position - float64(lower)
	return sortedValues[lower]*(1-fraction) + sortedValues[upper]*fraction
}

func populationStdDev(values []float64) float64 {
	mean := averageFloat64(values)
	sum := 0.0
	for _, value := range values {
		delta := value - mean
		sum += delta * delta
	}
	return math.Sqrt(sum / float64(len(values)))
}

func publicRawMetricValue(metricName string, value float64, fillEmpty bool) *float64 {
	if isNullPingMetricValue(metricName, value, fillEmpty) {
		return nil
	}
	return publicMetricValue(value)
}

func isNullPingMetricValue(metricName string, value float64, fillEmpty bool) bool {
	if !fillEmpty || value != -1 {
		return false
	}
	return metricName == metricstore.MetricPingLatency || metricName == metricstore.MetricPingLoss
}

func metricDownsampleInterval(rangeDuration time.Duration, maxPoints int) time.Duration {
	if maxPoints <= 0 {
		maxPoints = defaultMetricQueryPoints
	}
	nanos := rangeDuration.Nanoseconds()
	if nanos <= 0 {
		return time.Second
	}
	interval := time.Duration((nanos + int64(maxPoints) - 1) / int64(maxPoints))
	if interval < time.Second {
		return time.Second
	}
	return metric.CeilStandardInterval(interval)
}

// metricWindowAggregateInterval returns an epoch-aligned interval large enough
// to place every timestamp from the Unix epoch through end in one bucket. Using
// whole hours keeps the interval compatible with every retained rollup tier.

func metricWindowAggregateInterval(end time.Time) time.Duration {
	const secondsPerHour = int64(time.Hour / time.Second)
	maxHours := int64(^uint64(0)>>1) / int64(time.Hour)
	hours := end.UTC().Unix()/secondsPerHour + 1
	if hours < 1 {
		hours = 1
	}
	if hours > maxHours {
		hours = maxHours
	}
	return time.Duration(hours) * time.Hour
}
