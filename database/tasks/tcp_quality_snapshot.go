package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/metricstore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/pkg/metric"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils"
)

var tcpQualitySnapshotWindows = []int{1, 6, 12, 24, 72, 168}

type tcpQualityObservation struct {
	Client          string
	CatalogRevision string
	FinishedAt      time.Time
	Result          v2.TCPQualityTargetResult
}

type tcpQualityAggregate struct {
	Sent       int
	Received   int
	Runs       int
	MinSamples []weightedValue
	MaxSamples []weightedValue
	AvgSamples []weightedValue
	P50Samples []weightedValue
	P95Samples []weightedValue
}

type weightedValue struct {
	Value  float64
	Weight int
}

type tcpQualityModeStats struct {
	LossPercent     float64            `json:"loss_percent"`
	Min             float64            `json:"min_ms"`
	Max             float64            `json:"max_ms"`
	Average         float64            `json:"average_ms"`
	P50             float64            `json:"p50_ms"`
	P95             float64            `json:"p95_ms"`
	SamplesSent     int                `json:"samples_sent"`
	SamplesReceived int                `json:"samples_received"`
	Runs            int                `json:"runs"`
	CoveragePercent float64            `json:"coverage_percent"`
	Score           *float64           `json:"score"`
	ScoreComponents map[string]float64 `json:"score_components,omitempty"`
	ScoreInputs     map[string]float64 `json:"score_inputs,omitempty"`
	Rankable        bool               `json:"rankable"`
	Reason          string             `json:"reason,omitempty"`
}

type tcpQualityNodeTarget struct {
	TargetKey string               `json:"target_key"`
	Standard  *tcpQualityModeStats `json:"standard,omitempty"`
	Large     *tcpQualityModeStats `json:"large,omitempty"`
}

type tcpQualityTrendPoint struct {
	Time            time.Time `json:"time"`
	LossPercent     float64   `json:"loss_percent"`
	Min             float64   `json:"min_ms"`
	Max             float64   `json:"max_ms"`
	Average         float64   `json:"average_ms"`
	P50             float64   `json:"p50_ms"`
	P95             float64   `json:"p95_ms"`
	SamplesSent     int       `json:"samples_sent"`
	SamplesReceived int       `json:"samples_received"`
}

type tcpQualitySnapshotNode struct {
	UUID                    string                 `json:"uuid"`
	Name                    string                 `json:"name"`
	Region                  string                 `json:"region"`
	PublicRemark            string                 `json:"public_remark,omitempty"`
	Rank                    *int                   `json:"rank"`
	Grade                   string                 `json:"grade"`
	Rankable                bool                   `json:"rankable"`
	Reason                  string                 `json:"reason,omitempty"`
	ICMPScore               *float64               `json:"icmp_score"`
	TCPStandardScore        *float64               `json:"tcp_standard_score"`
	LargeScore              *float64               `json:"large_experimental_score"`
	TCPScore                *float64               `json:"tcp_score"`
	OverallScore            *float64               `json:"overall_score"`
	TCPScoreBeforeGuard     *float64               `json:"tcp_score_before_guard"`
	OverallScoreBeforeGuard *float64               `json:"overall_score_before_guard"`
	LossGuardCap            *float64               `json:"loss_guard_cap"`
	Diagnostics             []string               `json:"diagnostics"`
	Standard                tcpQualityModeStats    `json:"standard"`
	Large                   *tcpQualityModeStats   `json:"large,omitempty"`
	Targets                 []tcpQualityNodeTarget `json:"targets"`
	Trend                   []tcpQualityTrendPoint `json:"trend"`
	LargeTrend              []tcpQualityTrendPoint `json:"large_trend,omitempty"`
}

type tcpQualityScoreModel struct {
	Version string `json:"version"`
	Weights any    `json:"weights"`
	Guards  any    `json:"guards"`
}

type tcpQualitySnapshot struct {
	TaskID             uint                          `json:"task_id"`
	TaskName           string                        `json:"task_name"`
	WindowHours        int                           `json:"window_hours"`
	GeneratedAt        time.Time                     `json:"generated_at"`
	CatalogRevision    string                        `json:"catalog_revision"`
	ObservedRevisions  []string                      `json:"observed_catalog_revisions"`
	Targets            []utils.TCPQualityTargetLabel `json:"targets"`
	ExcludedTargetKeys []string                      `json:"excluded_target_keys"`
	Nodes              []tcpQualitySnapshotNode      `json:"nodes"`
	ValidNodes         int                           `json:"valid_nodes"`
	BestNodeUUID       string                        `json:"best_node_uuid,omitempty"`
	ScoreModel         tcpQualityScoreModel          `json:"score_model"`
	Privacy            string                        `json:"privacy"`
}

func RefreshTCPQualitySnapshots(ctx context.Context) error {
	taskList, err := GetAllTCPQualityTasks()
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	for _, task := range taskList {
		if !task.Enabled {
			continue
		}
		for _, hours := range tcpQualitySnapshotWindows {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			snapshot, err := buildTCPQualitySnapshot(ctx, task, hours, now)
			if err != nil {
				return fmt.Errorf("build TCP quality task %d window %dh: %w", task.Id, hours, err)
			}
			payload, err := json.Marshal(snapshot)
			if err != nil {
				return err
			}
			if err := SaveTCPQualitySnapshot(ctx, models.TCPQualitySnapshot{
				TaskID:      task.Id,
				WindowHours: hours,
				Payload:     string(payload),
				GeneratedAt: now,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func buildTCPQualitySnapshot(ctx context.Context, task models.TCPQualityTask, hours int, now time.Time) (tcpQualitySnapshot, error) {
	scoreConfig := loadTCPQualityScoreConfig()
	start := now.Add(-time.Duration(hours) * time.Hour)
	labels, currentRevision, err := utils.GetTCPQualityTargetLabels(ctx, task)
	if err != nil {
		return tcpQualitySnapshot{}, err
	}
	labelKeys := make(map[string]struct{}, len(labels))
	for _, label := range labels {
		labelKeys[label.Key] = struct{}{}
	}
	runs, err := ListTCPQualityRuns(ctx, task.Id, start)
	if err != nil {
		return tcpQualitySnapshot{}, err
	}
	observations := make([]tcpQualityObservation, 0)
	revisions := make(map[string]struct{})
	for _, run := range runs {
		results, err := DecodeTCPQualityResults(run.Payload)
		if err != nil {
			continue
		}
		revisions[run.CatalogRevision] = struct{}{}
		for _, result := range results {
			if !tcpQualityResultUsable(result) {
				continue
			}
			if _, selected := labelKeys[result.TargetKey]; !selected {
				continue
			}
			observations = append(observations, tcpQualityObservation{
				Client:          run.Client,
				CatalogRevision: run.CatalogRevision,
				FinishedAt:      run.FinishedAt.UTC(),
				Result:          result,
			})
		}
	}

	clientMap, clientOrder, err := tcpQualityClients(task.Clients)
	if err != nil {
		return tcpQualitySnapshot{}, err
	}
	excludedBuckets, excludedTargets := detectTCPQualityReferenceOutages(task, observations, len(clientOrder), scoreConfig)
	filtered := observations[:0]
	for _, observation := range observations {
		if _, excluded := excludedBuckets[tcpQualityOutageBucketKey(task, observation)]; excluded {
			continue
		}
		filtered = append(filtered, observation)
	}
	observations = filtered

	nodeTargets := aggregateTCPQualityObservations(task, hours, observations, scoreConfig)
	scoreTCPQualityTargets(task, clientOrder, labels, nodeTargets, scoreConfig)
	icmpScores, err := buildTCPQualityICMPScores(ctx, task, start, now, clientOrder)
	if err != nil {
		return tcpQualitySnapshot{}, err
	}

	snapshot := tcpQualitySnapshot{
		TaskID:             task.Id,
		TaskName:           task.Name,
		WindowHours:        hours,
		GeneratedAt:        now,
		CatalogRevision:    currentRevision,
		Targets:            labels,
		ExcludedTargetKeys: sortedSetKeys(excludedTargets),
		ScoreModel: tcpQualityScoreModel{
			Version: "tcp-quality-v5",
			Weights: map[string]any{
				"overall_with_large": map[string]float64{
					"icmp": scoreConfig.OverallICMPWeight, "tcp_standard": scoreConfig.OverallStandardWeight,
					"large_experimental": scoreConfig.OverallLargeWeight,
				},
				"overall_without_large": map[string]float64{
					"icmp": scoreConfig.OverallICMPWeight, "tcp_standard": scoreConfig.OverallStandardWeight,
				},
				"tcp_standard": map[string]float64{
					"first_response_loss": scoreConfig.StandardLossWeight, "p50": scoreConfig.StandardP50Weight,
					"p95": scoreConfig.StandardP95Weight, "coverage": scoreConfig.StandardCoverageWeight,
				},
				"large_experimental": map[string]float64{
					"loss": scoreConfig.LargeLossWeight, "extra_loss": scoreConfig.LargeExtraLossWeight,
					"p95_degradation": scoreConfig.LargeP95DegradationWeight, "coverage": scoreConfig.LargeCoverageWeight,
				},
				"target_profile": map[string]float64{"mean": scoreConfig.ProfileMeanWeight, "p20": scoreConfig.ProfileP20Weight},
			},
			Guards: map[string]any{
				"minimum_runs": scoreConfig.MinimumRuns, "minimum_standard_samples": scoreConfig.MinimumStandardSamples,
				"minimum_large_samples":                  scoreConfig.MinimumLargeSamples,
				"minimum_target_coverage_percent":        scoreConfig.MinimumTargetCoveragePercent,
				"simultaneous_reference_failure_percent": scoreConfig.ReferenceFailurePercent,
				"loss_score_caps": map[string]float64{
					"warning_loss_percent": scoreConfig.GuardWarningLossPercent, "warning_maximum_score": scoreConfig.GuardWarningMaximumScore,
					"critical_loss_percent": scoreConfig.GuardCriticalLossPercent, "critical_maximum_score": scoreConfig.GuardCriticalMaximumScore,
					"severe_loss_percent": scoreConfig.GuardSevereLossPercent, "severe_maximum_score": scoreConfig.GuardSevereMaximumScore,
				},
				"grade_thresholds": map[string]float64{
					"excellent": scoreConfig.ExcellentThreshold, "good": scoreConfig.GoodThreshold, "fair": scoreConfig.FairThreshold,
				},
			},
		},
		Privacy: "Public snapshots contain target labels only; concrete IP addresses, hostnames and ports are omitted.",
	}
	for revision := range revisions {
		snapshot.ObservedRevisions = append(snapshot.ObservedRevisions, revision)
	}
	sort.Strings(snapshot.ObservedRevisions)

	for _, uuid := range clientOrder {
		client := clientMap[uuid]
		node := buildTCPQualitySnapshotNode(task, hours, client, labels, nodeTargets[uuid], icmpScores[uuid], observations, scoreConfig)
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	rankTCPQualityNodes(snapshot.Nodes)
	for _, node := range snapshot.Nodes {
		if node.Rankable {
			snapshot.ValidNodes++
		}
		if node.Rank != nil && *node.Rank == 1 {
			snapshot.BestNodeUUID = node.UUID
		}
	}
	return snapshot, nil
}

func tcpQualityResultUsable(result v2.TCPQualityTargetResult) bool {
	switch result.ErrorCode {
	case "", "partial_loss", "no_response":
		return true
	default:
		return false
	}
}

func tcpQualityClients(clientIDs []string) (map[string]models.Client, []string, error) {
	var clients []models.Client
	if len(clientIDs) == 0 {
		return map[string]models.Client{}, nil, nil
	}
	if err := dbcore.GetDBInstance().Where("uuid IN ? AND hidden = ?", clientIDs, false).Find(&clients).Error; err != nil {
		return nil, nil, err
	}
	clientMap := make(map[string]models.Client, len(clients))
	order := make([]string, 0, len(clients))
	for _, client := range clients {
		clientMap[client.UUID] = client
		order = append(order, client.UUID)
	}
	sort.Slice(order, func(i, j int) bool {
		left, right := clientMap[order[i]], clientMap[order[j]]
		if left.Weight != right.Weight {
			return left.Weight < right.Weight
		}
		return left.Name < right.Name
	})
	return clientMap, order, nil
}

func detectTCPQualityReferenceOutages(task models.TCPQualityTask, observations []tcpQualityObservation, clientCount int, scoreConfig tcpQualityScoreConfig) (map[string]struct{}, map[string]struct{}) {
	type counts struct{ reported, failed map[string]struct{} }
	grouped := make(map[string]*counts)
	for _, observation := range observations {
		key := tcpQualityOutageBucketKey(task, observation)
		group := grouped[key]
		if group == nil {
			group = &counts{reported: map[string]struct{}{}, failed: map[string]struct{}{}}
			grouped[key] = group
		}
		group.reported[observation.Client] = struct{}{}
		if observation.Result.LossRatio >= 0.90 {
			group.failed[observation.Client] = struct{}{}
		}
	}
	excludedBuckets := make(map[string]struct{})
	excludedTargets := make(map[string]struct{})
	minimumFailures := int(math.Ceil(float64(clientCount) * scoreConfig.ReferenceFailurePercent / 100))
	if minimumFailures < 2 {
		minimumFailures = 2
	}
	for key, group := range grouped {
		if len(group.failed) >= minimumFailures && len(group.reported) > 0 &&
			float64(len(group.failed))*100/float64(len(group.reported)) >= scoreConfig.ReferenceFailurePercent {
			excludedBuckets[key] = struct{}{}
			parts := strings.Split(key, "|")
			if len(parts) > 0 {
				excludedTargets[parts[0]] = struct{}{}
			}
		}
	}
	return excludedBuckets, excludedTargets
}

func tcpQualityOutageBucketKey(task models.TCPQualityTask, observation tcpQualityObservation) string {
	interval := int64(task.Interval)
	if interval < 60 {
		interval = 60
	}
	bucket := observation.FinishedAt.Unix() / interval
	return fmt.Sprintf("%s|%s|%d", observation.Result.TargetKey, observation.Result.Mode, bucket)
}

func aggregateTCPQualityObservations(task models.TCPQualityTask, hours int, observations []tcpQualityObservation, scoreConfig tcpQualityScoreConfig) map[string]map[string]map[string]*tcpQualityModeStats {
	type aggregateKey struct{ client, target, mode string }
	aggregates := make(map[aggregateKey]*tcpQualityAggregate)
	for _, observation := range observations {
		key := aggregateKey{observation.Client, observation.Result.TargetKey, observation.Result.Mode}
		aggregate := aggregates[key]
		if aggregate == nil {
			aggregate = &tcpQualityAggregate{}
			aggregates[key] = aggregate
		}
		result := observation.Result
		aggregate.Sent += result.SamplesSent
		aggregate.Received += result.SamplesReceived
		aggregate.Runs++
		if result.SamplesReceived > 0 {
			aggregate.MinSamples = append(aggregate.MinSamples, weightedValue{result.MinLatencyMS, result.SamplesReceived})
			aggregate.MaxSamples = append(aggregate.MaxSamples, weightedValue{result.MaxLatencyMS, result.SamplesReceived})
			aggregate.AvgSamples = append(aggregate.AvgSamples, weightedValue{result.AverageLatencyMS, result.SamplesReceived})
			aggregate.P50Samples = append(aggregate.P50Samples, weightedValue{result.P50LatencyMS, result.SamplesReceived})
			aggregate.P95Samples = append(aggregate.P95Samples, weightedValue{result.P95LatencyMS, result.SamplesReceived})
		}
	}
	expectedRuns := int(math.Floor(float64(hours*3600) / float64(task.Interval)))
	if expectedRuns < 1 {
		expectedRuns = 1
	}
	result := make(map[string]map[string]map[string]*tcpQualityModeStats)
	for key, aggregate := range aggregates {
		if result[key.client] == nil {
			result[key.client] = make(map[string]map[string]*tcpQualityModeStats)
		}
		if result[key.client][key.target] == nil {
			result[key.client][key.target] = make(map[string]*tcpQualityModeStats)
		}
		packets := task.StandardPackets
		if key.mode == "large" {
			packets = task.LargePackets
		}
		expectedSamples := expectedRuns * packets
		coverage := clampScore(float64(aggregate.Sent) * 100 / float64(expectedSamples))
		loss := 100.0
		if aggregate.Sent > 0 {
			loss = float64(aggregate.Sent-aggregate.Received) * 100 / float64(aggregate.Sent)
		}
		minimumSamples := scoreConfig.MinimumStandardSamples
		if key.mode == "large" {
			minimumSamples = scoreConfig.MinimumLargeSamples
		}
		stats := &tcpQualityModeStats{
			LossPercent:     roundScore(loss),
			Min:             roundScore(weightedQuantile(aggregate.MinSamples, 0.05)),
			Max:             roundScore(weightedQuantile(aggregate.MaxSamples, 0.95)),
			Average:         roundScore(weightedMean(aggregate.AvgSamples)),
			P50:             roundScore(weightedQuantile(aggregate.P50Samples, 0.50)),
			P95:             roundScore(weightedQuantile(aggregate.P95Samples, 0.95)),
			SamplesSent:     aggregate.Sent,
			SamplesReceived: aggregate.Received,
			Runs:            aggregate.Runs,
			CoveragePercent: roundScore(coverage),
			Rankable: aggregate.Runs >= scoreConfig.MinimumRuns && aggregate.Sent >= minimumSamples &&
				coverage >= scoreConfig.MinimumTargetCoveragePercent && aggregate.Received > 0,
		}
		if !stats.Rankable {
			stats.Reason = tcpQualityUnrankedReason(stats, scoreConfig, minimumSamples)
		}
		result[key.client][key.target][key.mode] = stats
	}
	return result
}

func scoreTCPQualityTargets(task models.TCPQualityTask, clients []string, labels []utils.TCPQualityTargetLabel, data map[string]map[string]map[string]*tcpQualityModeStats, scoreConfig tcpQualityScoreConfig) {
	for _, label := range labels {
		for _, mode := range []string{"standard", "large"} {
			for _, client := range clients {
				stats := data[client][label.Key][mode]
				if stats == nil || !stats.Rankable {
					continue
				}
				var score float64
				if mode == "standard" {
					components := map[string]float64{
						"first_response_loss": tcpLossScore(stats.LossPercent),
						"p50":                 tcpP50AbsoluteScore(stats.P50),
						"p95":                 tcpP95AbsoluteScore(stats.P95),
						"coverage":            stats.CoveragePercent,
					}
					score = weightedScore(
						[2]float64{components["first_response_loss"], scoreConfig.StandardLossWeight},
						[2]float64{components["p50"], scoreConfig.StandardP50Weight},
						[2]float64{components["p95"], scoreConfig.StandardP95Weight},
						[2]float64{components["coverage"], scoreConfig.StandardCoverageWeight},
					)
					stats.ScoreComponents = roundScoreMap(components)
				} else {
					standard := data[client][label.Key]["standard"]
					if standard == nil || !standard.Rankable {
						stats.Rankable = false
						stats.Reason = "缺少同目标标准包基准"
						continue
					}
					extraLoss := math.Max(0, stats.LossPercent-standard.LossPercent)
					ratio := stats.P95 / math.Max(standard.P95, 1)
					components := map[string]float64{
						"absolute_loss":   tcpLossScore(stats.LossPercent),
						"extra_loss":      tcpExtraLossScore(extraLoss),
						"p95_degradation": tcpLargeP95RatioScore(ratio),
						"coverage":        stats.CoveragePercent,
					}
					score = weightedScore(
						[2]float64{components["absolute_loss"], scoreConfig.LargeLossWeight},
						[2]float64{components["extra_loss"], scoreConfig.LargeExtraLossWeight},
						[2]float64{components["p95_degradation"], scoreConfig.LargeP95DegradationWeight},
						[2]float64{components["coverage"], scoreConfig.LargeCoverageWeight},
					)
					stats.ScoreComponents = roundScoreMap(components)
					stats.ScoreInputs = map[string]float64{
						"extra_loss_percent":    roundScore(extraLoss),
						"p95_degradation_ratio": roundScore(ratio),
					}
				}
				score = roundScore(score)
				stats.Score = &score
			}
		}
	}
}

func buildTCPQualitySnapshotNode(task models.TCPQualityTask, hours int, client models.Client, labels []utils.TCPQualityTargetLabel, targetData map[string]map[string]*tcpQualityModeStats, icmpScore *float64, observations []tcpQualityObservation, scoreConfig tcpQualityScoreConfig) tcpQualitySnapshotNode {
	node := tcpQualitySnapshotNode{
		UUID:         client.UUID,
		Name:         client.Name,
		Region:       client.Region,
		PublicRemark: client.PublicRemark,
		ICMPScore:    icmpScore,
		Grade:        "未评级",
	}
	standardScores, largeScores := []float64{}, []float64{}
	validTargets := 0
	availableTargets := 0
	var standardAggregate, largeAggregate tcpQualityAggregate
	for _, label := range labels {
		availableTargets++
		target := tcpQualityNodeTarget{TargetKey: label.Key}
		if modes := targetData[label.Key]; modes != nil {
			target.Standard = modes["standard"]
			target.Large = modes["large"]
		}
		if target.Standard != nil {
			accumulateModeStats(&standardAggregate, target.Standard)
			if target.Standard.Score != nil {
				standardScores = append(standardScores, *target.Standard.Score)
				validTargets++
			}
		}
		if target.Large != nil {
			accumulateModeStats(&largeAggregate, target.Large)
			if target.Large.Score != nil {
				largeScores = append(largeScores, *target.Large.Score)
			}
		}
		node.Targets = append(node.Targets, target)
	}
	node.Standard = profileModeStats(standardAggregate, standardScores, validTargets, availableTargets, scoreConfig, scoreConfig.MinimumStandardSamples)
	node.TCPStandardScore = node.Standard.Score
	if task.LargeEnabled {
		large := profileModeStats(largeAggregate, largeScores, len(largeScores), availableTargets, scoreConfig, scoreConfig.MinimumLargeSamples)
		if large.Rankable && node.Standard.Rankable {
			large.ScoreInputs["extra_loss_percent"] = roundScore(math.Max(0, large.LossPercent-node.Standard.LossPercent))
			large.ScoreInputs["p95_degradation_ratio"] = roundScore(large.P95 / math.Max(node.Standard.P95, 1))
		}
		node.Large = &large
		node.LargeScore = large.Score
	}
	if node.TCPStandardScore != nil {
		tcpScore := *node.TCPStandardScore
		if node.LargeScore != nil {
			tcpScore = weightedScore(
				[2]float64{*node.TCPStandardScore, scoreConfig.OverallStandardWeight},
				[2]float64{*node.LargeScore, scoreConfig.OverallLargeWeight},
			)
		}
		beforeGuard := roundScore(tcpScore)
		node.TCPScoreBeforeGuard = &beforeGuard
		tcpScore = applyTCPQualityLossGuard(beforeGuard, node.Standard.LossPercent, scoreConfig)
		tcpScore = roundScore(tcpScore)
		node.TCPScore = &tcpScore
	}
	if node.ICMPScore != nil && node.TCPStandardScore != nil {
		var overall float64
		if node.LargeScore != nil {
			overall = weightedScore(
				[2]float64{*node.ICMPScore, scoreConfig.OverallICMPWeight},
				[2]float64{*node.TCPStandardScore, scoreConfig.OverallStandardWeight},
				[2]float64{*node.LargeScore, scoreConfig.OverallLargeWeight},
			)
		} else {
			overall = weightedScore(
				[2]float64{*node.ICMPScore, scoreConfig.OverallICMPWeight},
				[2]float64{*node.TCPStandardScore, scoreConfig.OverallStandardWeight},
			)
		}
		beforeGuard := roundScore(overall)
		node.OverallScoreBeforeGuard = &beforeGuard
		overall = applyTCPQualityLossGuard(beforeGuard, node.Standard.LossPercent, scoreConfig)
		overall = roundScore(overall)
		node.OverallScore = &overall
	}
	node.LossGuardCap = tcpQualityLossGuardCap(node.Standard.LossPercent, scoreConfig)
	node.Diagnostics = buildTCPQualityDiagnostics(node)
	node.Rankable = node.OverallScore != nil
	if !node.Rankable {
		node.Reason = node.Standard.Reason
		if node.ICMPScore == nil {
			node.Reason = joinReason(node.Reason, "ICMP 基础检测数据不足")
		}
	} else {
		node.Grade = tcpQualityGrade(*node.OverallScore, scoreConfig)
	}
	node.Trend, node.LargeTrend = buildTCPQualityTrends(task, hours, client.UUID, observations)
	if !task.LargeEnabled {
		node.LargeTrend = nil
	}
	return node
}

func accumulateModeStats(aggregate *tcpQualityAggregate, stats *tcpQualityModeStats) {
	aggregate.Sent += stats.SamplesSent
	aggregate.Received += stats.SamplesReceived
	aggregate.Runs += stats.Runs
	if stats.SamplesReceived > 0 {
		aggregate.MinSamples = append(aggregate.MinSamples, weightedValue{stats.Min, stats.SamplesReceived})
		aggregate.MaxSamples = append(aggregate.MaxSamples, weightedValue{stats.Max, stats.SamplesReceived})
		aggregate.AvgSamples = append(aggregate.AvgSamples, weightedValue{stats.Average, stats.SamplesReceived})
		aggregate.P50Samples = append(aggregate.P50Samples, weightedValue{stats.P50, stats.SamplesReceived})
		aggregate.P95Samples = append(aggregate.P95Samples, weightedValue{stats.P95, stats.SamplesReceived})
	}
}

func profileModeStats(aggregate tcpQualityAggregate, scores []float64, validTargets, availableTargets int, scoreConfig tcpQualityScoreConfig, minimumSamples int) tcpQualityModeStats {
	coverage := 0.0
	if availableTargets > 0 {
		coverage = float64(validTargets) * 100 / float64(availableTargets)
	}
	loss := 100.0
	if aggregate.Sent > 0 {
		loss = float64(aggregate.Sent-aggregate.Received) * 100 / float64(aggregate.Sent)
	}
	result := tcpQualityModeStats{
		LossPercent:     roundScore(loss),
		Min:             roundScore(weightedQuantile(aggregate.MinSamples, 0.05)),
		Max:             roundScore(weightedQuantile(aggregate.MaxSamples, 0.95)),
		Average:         roundScore(weightedMean(aggregate.AvgSamples)),
		P50:             roundScore(weightedQuantile(aggregate.P50Samples, 0.50)),
		P95:             roundScore(weightedQuantile(aggregate.P95Samples, 0.95)),
		SamplesSent:     aggregate.Sent,
		SamplesReceived: aggregate.Received,
		Runs:            aggregate.Runs,
		CoveragePercent: roundScore(coverage),
		Rankable: len(scores) > 0 && coverage >= scoreConfig.MinimumTargetCoveragePercent &&
			aggregate.Sent >= minimumSamples,
	}
	if result.Rankable {
		meanScore := mean(scores)
		p20Score := quantile(scores, 0.20)
		score := roundScore(weightedScore(
			[2]float64{meanScore, scoreConfig.ProfileMeanWeight},
			[2]float64{p20Score, scoreConfig.ProfileP20Weight},
		))
		result.Score = &score
		result.ScoreComponents = roundScoreMap(map[string]float64{
			"target_mean": meanScore,
			"target_p20":  p20Score,
		})
		result.ScoreInputs = map[string]float64{
			"valid_targets":     float64(validTargets),
			"available_targets": float64(availableTargets),
		}
	} else {
		result.Reason = tcpQualityUnrankedReason(&result, scoreConfig, minimumSamples)
	}
	return result
}

func buildTCPQualityTrends(task models.TCPQualityTask, hours int, uuid string, observations []tcpQualityObservation) ([]tcpQualityTrendPoint, []tcpQualityTrendPoint) {
	windowSeconds := int64(hours * 3600)
	bucketSeconds := int64(task.Interval)
	if minimum := windowSeconds / 120; bucketSeconds < minimum {
		bucketSeconds = minimum
	}
	if bucketSeconds < 60 {
		bucketSeconds = 60
	}
	bucketsByMode := map[string]map[int64]*tcpQualityAggregate{
		"standard": {},
		"large":    {},
	}
	for _, observation := range observations {
		if observation.Client != uuid {
			continue
		}
		buckets, supportedMode := bucketsByMode[observation.Result.Mode]
		if !supportedMode {
			continue
		}
		bucket := observation.FinishedAt.Unix() / bucketSeconds * bucketSeconds
		aggregate := buckets[bucket]
		if aggregate == nil {
			aggregate = &tcpQualityAggregate{}
			buckets[bucket] = aggregate
		}
		aggregate.Sent += observation.Result.SamplesSent
		aggregate.Received += observation.Result.SamplesReceived
		if observation.Result.SamplesReceived > 0 {
			aggregate.MinSamples = append(aggregate.MinSamples, weightedValue{observation.Result.MinLatencyMS, observation.Result.SamplesReceived})
			aggregate.MaxSamples = append(aggregate.MaxSamples, weightedValue{observation.Result.MaxLatencyMS, observation.Result.SamplesReceived})
			aggregate.AvgSamples = append(aggregate.AvgSamples, weightedValue{observation.Result.AverageLatencyMS, observation.Result.SamplesReceived})
			aggregate.P50Samples = append(aggregate.P50Samples, weightedValue{observation.Result.P50LatencyMS, observation.Result.SamplesReceived})
			aggregate.P95Samples = append(aggregate.P95Samples, weightedValue{observation.Result.P95LatencyMS, observation.Result.SamplesReceived})
		}
	}
	return buildTCPQualityTrendPoints(bucketsByMode["standard"]), buildTCPQualityTrendPoints(bucketsByMode["large"])
}

func buildTCPQualityTrendPoints(buckets map[int64]*tcpQualityAggregate) []tcpQualityTrendPoint {
	keys := make([]int64, 0, len(buckets))
	for key := range buckets {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	result := make([]tcpQualityTrendPoint, 0, len(keys))
	for _, key := range keys {
		aggregate := buckets[key]
		loss := 0.0
		if aggregate.Sent > 0 {
			loss = float64(aggregate.Sent-aggregate.Received) * 100 / float64(aggregate.Sent)
		}
		result = append(result, tcpQualityTrendPoint{
			Time:            time.Unix(key, 0).UTC(),
			LossPercent:     roundScore(loss),
			Min:             roundScore(weightedQuantile(aggregate.MinSamples, 0.05)),
			Max:             roundScore(weightedQuantile(aggregate.MaxSamples, 0.95)),
			Average:         roundScore(weightedMean(aggregate.AvgSamples)),
			P50:             roundScore(weightedQuantile(aggregate.P50Samples, 0.50)),
			P95:             roundScore(weightedQuantile(aggregate.P95Samples, 0.95)),
			SamplesSent:     aggregate.Sent,
			SamplesReceived: aggregate.Received,
		})
	}
	if len(result) > 120 {
		result = result[len(result)-120:]
	}
	return result
}

type icmpNodeAggregate struct {
	LossWeighted float64
	Samples      int
	Expected     int
	P50Values    []float64
	P95Values    []float64
}

func buildTCPQualityICMPScores(ctx context.Context, task models.TCPQualityTask, start, end time.Time, clients []string) (map[string]*float64, error) {
	result := make(map[string]*float64)
	if len(task.ICMPTaskIDs) == 0 {
		return result, nil
	}
	store := metricstore.GetStore()
	if store == nil {
		return result, nil
	}
	var pingTasks []models.PingTask
	ids := make([]uint, 0, len(task.ICMPTaskIDs))
	for _, raw := range task.ICMPTaskIDs {
		id, _ := strconv.ParseUint(raw, 10, 32)
		ids = append(ids, uint(id))
	}
	if err := dbcore.GetDBInstance().Where("id IN ? AND type = ?", ids, "icmp").Find(&pingTasks).Error; err != nil {
		return nil, err
	}
	aggregates := make(map[string]*icmpNodeAggregate)
	for _, pingTask := range pingTasks {
		taskID := strconv.FormatUint(uint64(pingTask.Id), 10)
		loss, err := tcpQualityMetricSeries(ctx, store, metricstore.MetricPingLoss, metric.AggAvg, start, end, taskID)
		if err != nil {
			return nil, err
		}
		p50, err := tcpQualityMetricSeries(ctx, store, metricstore.MetricPingLatency, metric.AggP50, start, end, taskID)
		if err != nil {
			return nil, err
		}
		p95, err := tcpQualityMetricSeries(ctx, store, metricstore.MetricPingLatency, metric.AggP95, start, end, taskID)
		if err != nil {
			return nil, err
		}
		expected := int(math.Floor(end.Sub(start).Seconds() / float64(pingTask.Interval)))
		if expected < 1 {
			expected = 1
		}
		for _, uuid := range clients {
			lossPoint, hasLoss := loss[uuid]
			p50Point, hasP50 := p50[uuid]
			p95Point, hasP95 := p95[uuid]
			if !hasLoss || !hasP50 || !hasP95 || p50Point.Value < 0 || p95Point.Value < 0 {
				continue
			}
			aggregate := aggregates[uuid]
			if aggregate == nil {
				aggregate = &icmpNodeAggregate{}
				aggregates[uuid] = aggregate
			}
			aggregate.LossWeighted += clamp01(lossPoint.Value) * float64(lossPoint.Count)
			aggregate.Samples += lossPoint.Count
			aggregate.Expected += expected
			aggregate.P50Values = append(aggregate.P50Values, p50Point.Value)
			aggregate.P95Values = append(aggregate.P95Values, p95Point.Value)
		}
	}
	type icmpCandidate struct {
		uuid, reason                         string
		loss, p50, p95, volatility, coverage float64
	}
	candidates := make([]icmpCandidate, 0, len(aggregates))
	for uuid, aggregate := range aggregates {
		if aggregate.Samples == 0 || len(aggregate.P50Values) == 0 {
			continue
		}
		loss := aggregate.LossWeighted * 100 / float64(aggregate.Samples)
		p50 := mean(aggregate.P50Values)
		p95 := mean(aggregate.P95Values)
		coverage := clampScore(float64(aggregate.Samples) * 100 / float64(maxInt(aggregate.Expected, 1)))
		candidate := icmpCandidate{
			uuid: uuid, loss: loss, p50: p50, p95: p95,
			volatility: math.Max(0, p95-p50) / math.Max(p50, 10),
			coverage:   coverage,
		}
		if aggregate.Samples < 3 || coverage < 80 {
			candidate.reason = "insufficient"
		}
		candidates = append(candidates, candidate)
	}
	p50Values, p95Values := []float64{}, []float64{}
	for _, candidate := range candidates {
		if candidate.reason == "" {
			p50Values = append(p50Values, candidate.p50)
			p95Values = append(p95Values, candidate.p95)
		}
	}
	if len(p50Values) < 2 {
		return result, nil
	}
	for _, candidate := range candidates {
		if candidate.reason != "" {
			continue
		}
		score := tcpICMPLossScore(candidate.loss)*0.40 +
			robustRelativeScore(p50Values, candidate.p50)*0.30 +
			robustRelativeScore(p95Values, candidate.p95)*0.25 +
			tcpICMPVolatilityScore(candidate.volatility)*0.03 +
			candidate.coverage*0.02
		score = roundScore(score)
		result[candidate.uuid] = &score
	}
	return result, nil
}

type tcpQualityMetricAggregate struct {
	Value float64
	Count int
}

func tcpQualityMetricSeries(ctx context.Context, store *metric.Store, metricName string, aggregation metric.Aggregation, start, end time.Time, taskID string) (map[string]tcpQualityMetricAggregate, error) {
	points, err := store.Series(ctx, metric.AggregateQuery{
		Query: metric.Query{
			MetricName: metricName,
			Start:      start,
			End:        end,
			Tags:       map[string]string{"task_id": taskID},
			Order:      metric.OrderAsc,
		},
		Aggregation:    aggregation,
		Interval:       end.Sub(start),
		PreserveSeries: true,
	}, end)
	if err != nil {
		return nil, err
	}
	grouped := make(map[string][]weightedValue)
	for _, point := range points {
		if point.EntityID == "" || point.Count <= 0 {
			continue
		}
		grouped[point.EntityID] = append(grouped[point.EntityID], weightedValue{point.Value, point.Count})
	}
	result := make(map[string]tcpQualityMetricAggregate, len(grouped))
	for uuid, values := range grouped {
		count := 0
		for _, value := range values {
			count += value.Weight
		}
		result[uuid] = tcpQualityMetricAggregate{Value: weightedMean(values), Count: count}
	}
	return result, nil
}

func rankTCPQualityNodes(nodes []tcpQualitySnapshotNode) {
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Rankable != nodes[j].Rankable {
			return nodes[i].Rankable
		}
		left, right := tcpQualityRankingScore(nodes[i]), tcpQualityRankingScore(nodes[j])
		if left != right {
			return left > right
		}
		if nodes[i].Standard.LossPercent != nodes[j].Standard.LossPercent {
			return nodes[i].Standard.LossPercent < nodes[j].Standard.LossPercent
		}
		return nodes[i].Name < nodes[j].Name
	})
	rank := 0
	rankedPosition := 0
	var last *float64
	for index := range nodes {
		score := tcpQualityRankingScore(nodes[index])
		if !nodes[index].Rankable || score < 0 {
			nodes[index].Rank = nil
			continue
		}
		rankedPosition++
		if last == nil || math.Abs(score-*last) > 1e-9 {
			rank = rankedPosition
			value := score
			last = &value
		}
		value := rank
		nodes[index].Rank = &value
	}
}

func tcpQualityRankingScore(node tcpQualitySnapshotNode) float64 {
	if node.OverallScore != nil {
		return *node.OverallScore
	}
	if node.TCPScore != nil {
		return *node.TCPScore
	}
	return -1
}

func weightedQuantile(values []weightedValue, percentile float64) float64 {
	filtered := make([]weightedValue, 0, len(values))
	total := 0
	for _, value := range values {
		if value.Weight > 0 && !math.IsNaN(value.Value) && !math.IsInf(value.Value, 0) {
			filtered = append(filtered, value)
			total += value.Weight
		}
	}
	if total == 0 {
		return 0
	}
	sort.Slice(filtered, func(i, j int) bool { return filtered[i].Value < filtered[j].Value })
	target := int(math.Ceil(float64(total) * percentile))
	if target < 1 {
		target = 1
	}
	current := 0
	for _, value := range filtered {
		current += value.Weight
		if current >= target {
			return value.Value
		}
	}
	return filtered[len(filtered)-1].Value
}

func weightedMean(values []weightedValue) float64 {
	total, weight := 0.0, 0
	for _, value := range values {
		total += value.Value * float64(value.Weight)
		weight += value.Weight
	}
	if weight == 0 {
		return 0
	}
	return total / float64(weight)
}

func quantile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	ordered := append([]float64(nil), values...)
	sort.Float64s(ordered)
	position := float64(len(ordered)-1) * percentile
	lower, upper := int(math.Floor(position)), int(math.Ceil(position))
	if lower == upper {
		return ordered[lower]
	}
	fraction := position - float64(lower)
	return ordered[lower]*(1-fraction) + ordered[upper]*fraction
}

func robustRelativeScore(values []float64, value float64) float64 {
	if len(values) < 2 {
		return 100
	}
	lower, upper := quantile(values, 0.10), quantile(values, 0.90)
	if upper-lower < 1e-9 {
		return 100
	}
	return clampScore(100 * (1 - (value-lower)/(upper-lower)))
}

func piecewiseScore(value float64, points [][2]float64) float64 {
	if value <= points[0][0] {
		return points[0][1]
	}
	for index := 1; index < len(points); index++ {
		left, right := points[index-1], points[index]
		if value <= right[0] {
			ratio := (value - left[0]) / (right[0] - left[0])
			return left[1] + ratio*(right[1]-left[1])
		}
	}
	return points[len(points)-1][1]
}

func tcpLossScore(value float64) float64 {
	return piecewiseScore(value, [][2]float64{{0, 100}, {.5, 95}, {1, 90}, {2, 80}, {3, 70}, {5, 50}, {10, 20}, {20, 0}})
}

func tcpP50AbsoluteScore(value float64) float64 {
	if value > 350 {
		return 0
	}
	return piecewiseScore(value, [][2]float64{{50, 100}, {80, 95}, {120, 85}, {180, 70}, {250, 50}, {350, 30}})
}

func tcpP95AbsoluteScore(value float64) float64 {
	if value > 500 {
		return 0
	}
	return piecewiseScore(value, [][2]float64{{80, 100}, {120, 95}, {180, 85}, {250, 70}, {350, 50}, {500, 30}})
}

func tcpExtraLossScore(value float64) float64 {
	return piecewiseScore(value, [][2]float64{{0, 100}, {.5, 90}, {1, 80}, {2, 60}, {5, 20}, {10, 0}})
}

func tcpLargeP95RatioScore(value float64) float64 {
	return piecewiseScore(value, [][2]float64{{1.15, 100}, {1.30, 85}, {1.50, 65}, {2.00, 30}, {2.50, 0}})
}

func tcpICMPLossScore(value float64) float64 {
	return piecewiseScore(value, [][2]float64{{0, 100}, {.01, 98}, {.05, 94}, {.1, 90}, {.5, 75}, {1, 60}, {3, 35}, {5, 20}, {10, 0}})
}

func tcpICMPVolatilityScore(value float64) float64 {
	return piecewiseScore(value, [][2]float64{{0, 100}, {.05, 95}, {.10, 85}, {.20, 65}, {.50, 20}, {1, 0}})
}

func applyTCPQualityLossGuard(score, loss float64, scoreConfig tcpQualityScoreConfig) float64 {
	switch {
	case loss >= scoreConfig.GuardSevereLossPercent:
		return math.Min(score, scoreConfig.GuardSevereMaximumScore)
	case loss >= scoreConfig.GuardCriticalLossPercent:
		return math.Min(score, scoreConfig.GuardCriticalMaximumScore)
	case loss >= scoreConfig.GuardWarningLossPercent:
		return math.Min(score, scoreConfig.GuardWarningMaximumScore)
	default:
		return score
	}
}

func tcpQualityLossGuardCap(loss float64, scoreConfig tcpQualityScoreConfig) *float64 {
	var capValue float64
	switch {
	case loss >= scoreConfig.GuardSevereLossPercent:
		capValue = scoreConfig.GuardSevereMaximumScore
	case loss >= scoreConfig.GuardCriticalLossPercent:
		capValue = scoreConfig.GuardCriticalMaximumScore
	case loss >= scoreConfig.GuardWarningLossPercent:
		capValue = scoreConfig.GuardWarningMaximumScore
	default:
		return nil
	}
	capValue = roundScore(capValue)
	return &capValue
}

func buildTCPQualityDiagnostics(node tcpQualitySnapshotNode) []string {
	result := make([]string, 0, 3)
	if node.LossGuardCap != nil {
		result = append(result, fmt.Sprintf("标准 SYN 首次响应丢失 %.2f%%，综合分最高 %.1f", node.Standard.LossPercent, *node.LossGuardCap))
	} else if node.Standard.LossPercent >= 0.5 {
		result = append(result, fmt.Sprintf("标准 SYN 首次响应丢失 %.2f%%，是主要扣分项", node.Standard.LossPercent))
	}
	if node.Standard.P95 >= 180 && len(result) < 3 {
		result = append(result, fmt.Sprintf("标准 SYN P95 为 %.0fms，尾延迟偏高", node.Standard.P95))
	}
	if node.Large != nil && node.Large.Rankable && node.Standard.Rankable {
		extraLoss := math.Max(0, node.Large.LossPercent-node.Standard.LossPercent)
		ratio := node.Large.P95 / math.Max(node.Standard.P95, 1)
		if extraLoss >= 1 && len(result) < 3 {
			result = append(result, fmt.Sprintf("实验性大小包额外丢失 %.2f 个百分点", extraLoss))
		}
		if ratio >= 1.3 && len(result) < 3 {
			result = append(result, fmt.Sprintf("实验性大小包 P95 为标准 SYN 的 %.2f 倍", ratio))
		}
	}
	if components := node.Standard.ScoreComponents; len(result) < 3 && components != nil &&
		components["target_mean"]-components["target_p20"] >= 15 {
		result = append(result, "部分测试目标明显弱于平均水平")
	}
	if len(result) == 0 && node.Standard.Rankable {
		result = append(result, "未发现明显短板，分数来自各项轻微扣分")
	}
	return result
}

func tcpQualityGrade(score float64, scoreConfig tcpQualityScoreConfig) string {
	switch {
	case score >= scoreConfig.ExcellentThreshold:
		return "优秀"
	case score >= scoreConfig.GoodThreshold:
		return "良好"
	case score >= scoreConfig.FairThreshold:
		return "一般"
	default:
		return "较差"
	}
}

func tcpQualityUnrankedReason(stats *tcpQualityModeStats, scoreConfig tcpQualityScoreConfig, minimumSamples int) string {
	var reasons []string
	if stats.Runs < scoreConfig.MinimumRuns {
		reasons = append(reasons, fmt.Sprintf("少于 %d 次运行", scoreConfig.MinimumRuns))
	}
	if stats.SamplesSent < minimumSamples {
		reasons = append(reasons, fmt.Sprintf("少于 %d 个 SYN 样本", minimumSamples))
	}
	if stats.CoveragePercent < scoreConfig.MinimumTargetCoveragePercent {
		reasons = append(reasons, fmt.Sprintf("有效目标覆盖率低于 %.0f%%", scoreConfig.MinimumTargetCoveragePercent))
	}
	if stats.SamplesReceived == 0 {
		reasons = append(reasons, "没有收到首包响应")
	}
	return strings.Join(reasons, "；")
}

func joinReason(left, right string) string {
	if left == "" {
		return right
	}
	if right == "" {
		return left
	}
	return left + "；" + right
}

func sortedSetKeys(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func mean(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	return total / float64(len(values))
}

func clampScore(value float64) float64 {
	return math.Max(0, math.Min(100, value))
}

func clamp01(value float64) float64 {
	return math.Max(0, math.Min(1, value))
}

func roundScore(value float64) float64 {
	return math.Round(value*10000) / 10000
}

func roundScoreMap(values map[string]float64) map[string]float64 {
	result := make(map[string]float64, len(values))
	for key, value := range values {
		result[key] = roundScore(value)
	}
	return result
}

func maxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
