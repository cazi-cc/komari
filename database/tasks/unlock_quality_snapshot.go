package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
)

var unlockQualityWindows = []int{1, 6, 12, 24, 72, 168}
var unlockQualitySnapshotRefreshMu sync.Mutex

type unlockQualitySnapshot struct {
	TaskID       uint                        `json:"task_id"`
	TaskName     string                      `json:"task_name"`
	Service      string                      `json:"service"`
	WindowHours  int                         `json:"window_hours"`
	GeneratedAt  time.Time                   `json:"generated_at"`
	PathBindings []unlockQualityPathBinding  `json:"path_bindings"`
	Nodes        []unlockQualitySnapshotNode `json:"nodes"`
}

type unlockQualityPathBinding struct {
	PingTaskID   uint   `json:"ping_task_id"`
	ExitNodeUUID string `json:"exit_node_uuid"`
	Family       int    `json:"family"`
}

type unlockQualitySnapshotNode struct {
	UUID             string                     `json:"uuid"`
	Name             string                     `json:"name"`
	PublicRemark     string                     `json:"public_remark,omitempty"`
	Rank             *int                       `json:"rank"`
	Score            *float64                   `json:"score"`
	Grade            string                     `json:"grade"`
	System           unlockQualityRouteSummary  `json:"system"`
	Control          *unlockQualityRouteSummary `json:"control,omitempty"`
	FixedDiagnostic  *unlockQualityRouteSummary `json:"fixed_diagnostic,omitempty"`
	ImprovementScore *float64                   `json:"improvement_score,omitempty"`
}

type unlockQualityRouteSummary struct {
	RouteMode       string                    `json:"route_mode"`
	Status          string                    `json:"status"`
	Score           *float64                  `json:"score"`
	Grade           string                    `json:"grade"`
	CoveragePercent float64                   `json:"coverage_percent"`
	SamplesSent     int                       `json:"samples_sent"`
	SamplesReceived int                       `json:"samples_received"`
	FailurePercent  float64                   `json:"failure_percent"`
	DNSMS           float64                   `json:"dns_ms"`
	ConnectMS       float64                   `json:"connect_ms"`
	TLSMS           float64                   `json:"tls_ms"`
	TTFBP50MS       float64                   `json:"ttfb_p50_ms"`
	TTFBP95MS       float64                   `json:"ttfb_p95_ms"`
	TotalP50MS      float64                   `json:"total_p50_ms"`
	TotalP95MS      float64                   `json:"total_p95_ms"`
	JitterMS        float64                   `json:"jitter_ms"`
	ExitCountry     string                    `json:"exit_country,omitempty"`
	EdgeColo        string                    `json:"edge_colo,omitempty"`
	LatestAt        *time.Time                `json:"latest_at,omitempty"`
	Components      *unlockQualityScoreParts  `json:"components,omitempty"`
	Trend           []unlockQualityTrendPoint `json:"trend"`
}

type unlockQualityScoreParts struct {
	Unlock      float64 `json:"unlock"`
	Reliability float64 `json:"reliability"`
	TTFB        float64 `json:"ttfb"`
	Transport   float64 `json:"transport"`
	Stability   float64 `json:"stability"`
}

type unlockQualityTrendPoint struct {
	Time         time.Time `json:"time"`
	TTFBP50MS    float64   `json:"ttfb_p50_ms"`
	TTFBP95MS    float64   `json:"ttfb_p95_ms"`
	TTFBMinMS    float64   `json:"ttfb_min_ms"`
	TTFBMaxMS    float64   `json:"ttfb_max_ms"`
	FailureCount int       `json:"failure_count"`
	SamplesSent  int       `json:"samples_sent"`
}

func RefreshUnlockQualitySnapshots(ctx context.Context, force ...bool) error {
	unlockQualitySnapshotRefreshMu.Lock()
	defer unlockQualitySnapshotRefreshMu.Unlock()

	taskList, err := GetAllUnlockQualityTasks()
	if err != nil {
		return err
	}
	forceRefresh := len(force) > 0 && force[0]
	now := time.Now().UTC()
	pathBindings, err := loadUnlockQualityPathBindings()
	if err != nil {
		return err
	}
	var existing []models.UnlockQualitySnapshot
	if err := dbcore.GetDBInstance().WithContext(ctx).
		Select("task_id", "window_hours", "generated_at").
		Find(&existing).Error; err != nil {
		return err
	}
	generatedAt := make(map[string]time.Time, len(existing))
	for _, snapshot := range existing {
		generatedAt[unlockQualitySnapshotKey(snapshot.TaskID, snapshot.WindowHours)] = snapshot.GeneratedAt
	}

	for _, task := range taskList {
		dueWindows := make([]int, 0, len(unlockQualityWindows))
		maxHours := 0
		for _, hours := range unlockQualityWindows {
			lastGenerated := generatedAt[unlockQualitySnapshotKey(task.Id, hours)]
			if !forceRefresh && !lastGenerated.IsZero() &&
				now.Sub(lastGenerated) < unlockQualitySnapshotRefreshInterval(hours) {
				continue
			}
			dueWindows = append(dueWindows, hours)
			if hours > maxHours {
				maxHours = hours
			}
		}
		if len(dueWindows) == 0 {
			continue
		}

		runs, err := ListUnlockQualityRuns(ctx, task.Id, now.Add(-time.Duration(maxHours)*time.Hour))
		if err != nil {
			return err
		}
		clients, err := unlockQualityClients(task.Clients)
		if err != nil {
			return err
		}
		for _, hours := range dueWindows {
			snapshot := buildUnlockQualitySnapshot(task, clients, runs, pathBindings, hours, now)
			payload, err := json.Marshal(snapshot)
			if err != nil {
				return err
			}
			if err := SaveUnlockQualitySnapshot(ctx, models.UnlockQualitySnapshot{
				TaskID: task.Id, WindowHours: hours, Payload: string(payload), GeneratedAt: now,
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

func unlockQualitySnapshotKey(taskID uint, hours int) string {
	return fmt.Sprintf("%d:%d", taskID, hours)
}

func unlockQualitySnapshotRefreshInterval(hours int) time.Duration {
	switch {
	case hours <= 1:
		return time.Minute
	case hours <= 24:
		return 5 * time.Minute
	default:
		return 15 * time.Minute
	}
}

func unlockQualityClients(uuids []string) ([]models.Client, error) {
	if len(uuids) == 0 {
		return []models.Client{}, nil
	}
	var clients []models.Client
	err := dbcore.GetDBInstance().
		Select("uuid", "name", "public_remark", "weight", "hidden").
		Where("uuid IN ? AND hidden = ?", uuids, false).
		Order("weight DESC, name ASC").
		Find(&clients).Error
	return clients, err
}

func buildUnlockQualitySnapshot(task models.UnlockQualityTask, clients []models.Client, allRuns []models.UnlockQualityRun, pathBindings []unlockQualityPathBinding, hours int, now time.Time) unlockQualitySnapshot {
	start := now.Add(-time.Duration(hours) * time.Hour)
	byClient := make(map[string]map[string][]models.UnlockQualityRun)
	for _, run := range allRuns {
		if run.FinishedAt.Before(start) {
			continue
		}
		if byClient[run.Client] == nil {
			byClient[run.Client] = make(map[string][]models.UnlockQualityRun)
		}
		byClient[run.Client][run.RouteMode] = append(byClient[run.Client][run.RouteMode], run)
	}
	snapshot := unlockQualitySnapshot{
		TaskID: task.Id, TaskName: publicUnlockQualityServiceLabel(task.Service), Service: task.Service,
		WindowHours: hours, GeneratedAt: now,
		PathBindings: filterUnlockQualityPathBindings(pathBindings, clients),
		Nodes:        make([]unlockQualitySnapshotNode, 0, len(clients)),
	}
	for _, client := range clients {
		routeRuns := byClient[client.UUID]
		system := summarizeUnlockQualityRoute(task, "system", routeRuns["system"], hours, now)
		node := unlockQualitySnapshotNode{
			UUID: client.UUID, Name: client.Name, PublicRemark: client.PublicRemark,
			Score: system.Score, Grade: system.Grade, System: system,
		}
		if task.ControlEnabled {
			control := summarizeUnlockQualityRoute(task, "control", routeRuns["control"], hours, now)
			node.Control = &control
			if system.Score != nil && control.Score != nil {
				value := roundUnlockScore(*system.Score - *control.Score)
				node.ImprovementScore = &value
			}
		}
		if task.FixedEnabled {
			fixed := summarizeUnlockQualityRoute(task, "fixed", routeRuns["fixed"], hours, now)
			// Fixed-entry output is diagnostic only and never affects rank.
			fixed.Score = nil
			fixed.Grade = "诊断"
			fixed.Components = nil
			node.FixedDiagnostic = &fixed
		}
		snapshot.Nodes = append(snapshot.Nodes, node)
	}
	rankUnlockQualityNodes(snapshot.Nodes)
	return snapshot
}

func filterUnlockQualityPathBindings(bindings []unlockQualityPathBinding, clients []models.Client) []unlockQualityPathBinding {
	allowed := make(map[string]struct{}, len(clients))
	for _, client := range clients {
		allowed[client.UUID] = struct{}{}
	}
	filtered := make([]unlockQualityPathBinding, 0, len(bindings))
	for _, binding := range bindings {
		if _, ok := allowed[binding.ExitNodeUUID]; ok {
			filtered = append(filtered, binding)
		}
	}
	return filtered
}

func loadUnlockQualityPathBindings() ([]unlockQualityPathBinding, error) {
	var clients []models.Client
	if err := dbcore.GetDBInstance().
		Select("uuid", "ipv4", "ipv6", "reachable_addresses", "hidden").
		Where("hidden = ?", false).
		Find(&clients).Error; err != nil {
		return nil, err
	}
	var pingTasks []models.PingTask
	if err := dbcore.GetDBInstance().
		Select("id", "type", "target").
		Where("type = ?", "icmp").
		Find(&pingTasks).Error; err != nil {
		return nil, err
	}
	return buildUnlockQualityPathBindings(clients, pingTasks), nil
}

func buildUnlockQualityPathBindings(clients []models.Client, pingTasks []models.PingTask) []unlockQualityPathBinding {
	type endpoint struct {
		uuid   string
		family int
	}
	byAddress := make(map[string]endpoint, len(clients)*2)
	ambiguous := make(map[string]struct{})
	addEndpoint := func(value string, candidate endpoint) {
		address := normalizedUnlockQualityAddress(value)
		if address == "" {
			return
		}
		if _, conflict := ambiguous[address]; conflict {
			return
		}
		if existing, exists := byAddress[address]; exists && existing.uuid != candidate.uuid {
			delete(byAddress, address)
			ambiguous[address] = struct{}{}
			return
		}
		byAddress[address] = candidate
	}
	for _, client := range clients {
		for _, candidate := range []struct {
			value  string
			family int
		}{{client.IPv4, 4}, {client.IPv6, 6}} {
			addEndpoint(candidate.value, endpoint{uuid: client.UUID, family: candidate.family})
		}
		for _, value := range client.ReachableAddresses {
			address := net.ParseIP(strings.TrimSpace(strings.Trim(value, "[]")))
			if address == nil {
				continue
			}
			family := 6
			if address.To4() != nil {
				family = 4
			}
			addEndpoint(address.String(), endpoint{uuid: client.UUID, family: family})
		}
	}
	bindings := make([]unlockQualityPathBinding, 0)
	seen := make(map[string]struct{})
	for _, task := range pingTasks {
		if strings.ToLower(strings.TrimSpace(task.Type)) != "icmp" {
			continue
		}
		matched, ok := byAddress[normalizedUnlockQualityAddress(task.Target)]
		if !ok || matched.uuid == "" {
			continue
		}
		key := fmt.Sprintf("%d:%s:%d", task.Id, matched.uuid, matched.family)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		bindings = append(bindings, unlockQualityPathBinding{
			PingTaskID: task.Id, ExitNodeUUID: matched.uuid, Family: matched.family,
		})
	}
	sort.Slice(bindings, func(left, right int) bool {
		if bindings[left].ExitNodeUUID != bindings[right].ExitNodeUUID {
			return bindings[left].ExitNodeUUID < bindings[right].ExitNodeUUID
		}
		if bindings[left].Family != bindings[right].Family {
			return bindings[left].Family < bindings[right].Family
		}
		return bindings[left].PingTaskID < bindings[right].PingTaskID
	})
	return bindings
}

func normalizedUnlockQualityAddress(value string) string {
	value = strings.TrimSpace(strings.Trim(strings.TrimSpace(value), "[]"))
	parsed := net.ParseIP(value)
	if parsed == nil {
		return ""
	}
	return parsed.String()
}

func publicUnlockQualityServiceLabel(service string) string {
	if service == "chatgpt" {
		return "ChatGPT"
	}
	return "解锁线路"
}

func summarizeUnlockQualityRoute(task models.UnlockQualityTask, routeMode string, runs []models.UnlockQualityRun, hours int, now time.Time) unlockQualityRouteSummary {
	summary := unlockQualityRouteSummary{
		RouteMode: routeMode,
		Status:    "unknown",
		Grade:     "未评级",
		Trend:     []unlockQualityTrendPoint{},
	}
	if len(runs) == 0 {
		return summary
	}
	var ttfbP50Values, ttfbP95Values, totalP50Values, totalP95Values []float64
	var dnsTotal, connectTotal, tlsTotal float64
	var timingCount int
	var latestVerify *models.UnlockQualityRun
	var latestRun *models.UnlockQualityRun
	for index := range runs {
		run := &runs[index]
		summary.SamplesSent += run.SamplesSent
		summary.SamplesReceived += run.SamplesReceived
		if run.SamplesReceived > 0 {
			ttfbP50Values = append(ttfbP50Values, run.TTFBP50MS)
			ttfbP95Values = append(ttfbP95Values, run.TTFBP95MS)
			totalP50Values = append(totalP50Values, run.TotalP50MS)
			totalP95Values = append(totalP95Values, run.TotalP95MS)
			dnsTotal += run.DNSMS
			connectTotal += run.ConnectMS
			tlsTotal += run.TLSMS
			timingCount++
		}
		if latestRun == nil || run.FinishedAt.After(latestRun.FinishedAt) {
			latestRun = run
		}
		if run.ProbeKind == "verify" && (latestVerify == nil || run.FinishedAt.After(latestVerify.FinishedAt)) {
			latestVerify = run
		}
	}
	if latestRun != nil {
		at := latestRun.FinishedAt.UTC()
		summary.LatestAt = &at
	}
	if latestVerify != nil {
		summary.Status = normalizedUnlockQualityRunVerdict(*latestVerify)
		summary.ExitCountry = latestVerify.ExitCountry
		summary.EdgeColo = latestVerify.EdgeColo
		if now.Sub(latestVerify.FinishedAt) > time.Duration(task.VerifyInterval*2+60)*time.Second {
			summary.Status = "stale"
		}
	}
	if summary.SamplesSent > 0 {
		summary.FailurePercent = roundUnlockMetric(float64(summary.SamplesSent-summary.SamplesReceived) * 100 / float64(summary.SamplesSent))
	}
	if timingCount > 0 {
		summary.DNSMS = roundUnlockMetric(dnsTotal / float64(timingCount))
		summary.ConnectMS = roundUnlockMetric(connectTotal / float64(timingCount))
		summary.TLSMS = roundUnlockMetric(tlsTotal / float64(timingCount))
		summary.TTFBP50MS = roundUnlockMetric(unlockQualityQuantile(ttfbP50Values, 0.50))
		summary.TTFBP95MS = roundUnlockMetric(unlockQualityQuantile(ttfbP95Values, 0.95))
		summary.TotalP50MS = roundUnlockMetric(unlockQualityQuantile(totalP50Values, 0.50))
		summary.TotalP95MS = roundUnlockMetric(unlockQualityQuantile(totalP95Values, 0.95))
		summary.JitterMS = roundUnlockMetric(unlockQualityStdDev(ttfbP50Values))
	}
	expected := hours * 3600 / task.Interval * task.SampleCount
	if expected < 1 {
		expected = 1
	}
	summary.CoveragePercent = roundUnlockMetric(math.Min(100, float64(summary.SamplesSent)*100/float64(expected)))
	summary.Trend = buildUnlockQualityTrend(runs, hours)
	if summary.SamplesSent < 15 || summary.CoveragePercent < 80 ||
		summary.Status == "unknown" || summary.Status == "stale" {
		return summary
	}
	score, parts := scoreUnlockQualityRoute(summary)
	summary.Score = &score
	summary.Components = &parts
	summary.Grade = unlockQualityGrade(score)
	return summary
}

func normalizedUnlockQualityRunVerdict(run models.UnlockQualityRun) string {
	results, err := DecodeUnlockQualityResults(run.Payload)
	if err != nil {
		return run.Verdict
	}
	return normalizeUnlockQualityVerdict(results, run.ProbeKind, run.Verdict)
}

func scoreUnlockQualityRoute(summary unlockQualityRouteSummary) (float64, unlockQualityScoreParts) {
	unlockPoints := 0.0
	switch summary.Status {
	case "available":
		unlockPoints = 25
	case "partial":
		unlockPoints = 12.5
	}
	failureRatio := math.Max(0, math.Min(1, summary.FailurePercent/100))
	reliability := 30 * math.Pow(1-failureRatio, 3)
	ttfb := 16*unlockQualityLatencyFactor(summary.TTFBP50MS) +
		24*unlockQualityLatencyFactor(summary.TTFBP95MS)
	// TTFB is measured from request start, so it already includes DNS, TCP and
	// TLS. Keep those phases diagnostic-only instead of counting them twice.
	transport := 0.0
	tailSpread := math.Max(0, summary.TTFBP95MS-summary.TTFBP50MS)
	stability := 5 * unlockQualityTailFactor(tailSpread)
	parts := unlockQualityScoreParts{
		Unlock: roundUnlockScore(unlockPoints), Reliability: roundUnlockScore(reliability),
		TTFB: roundUnlockScore(ttfb), Transport: roundUnlockScore(transport),
		Stability: roundUnlockScore(stability),
	}
	score := unlockPoints + reliability + ttfb + transport + stability
	switch summary.Status {
	case "partial":
		score = math.Min(score, 69.9)
	case "region_limited", "unavailable":
		score = math.Min(score, 39.9)
	}
	switch {
	case summary.FailurePercent >= 10:
		score = math.Min(score, 49.9)
	case summary.FailurePercent >= 5:
		score = math.Min(score, 69.9)
	case summary.FailurePercent >= 1:
		score = math.Min(score, 84.9)
	}
	return roundUnlockScore(score), parts
}

func unlockQualityLatencyFactor(value float64) float64 {
	if value <= 0 {
		return 0
	}
	return unlockQualityPiecewiseFactor(value, []unlockQualityScorePoint{
		{Value: 25, Factor: 1},
		{Value: 50, Factor: 0.95},
		{Value: 100, Factor: 0.85},
		{Value: 200, Factor: 0.65},
		{Value: 500, Factor: 0.35},
		{Value: 1500, Factor: 0},
	})
}

func unlockQualityTailFactor(value float64) float64 {
	return unlockQualityPiecewiseFactor(value, []unlockQualityScorePoint{
		{Value: 10, Factor: 1},
		{Value: 20, Factor: 0.9},
		{Value: 50, Factor: 0.7},
		{Value: 100, Factor: 0.4},
		{Value: 300, Factor: 0},
	})
}

type unlockQualityScorePoint struct {
	Value  float64
	Factor float64
}

func unlockQualityPiecewiseFactor(value float64, points []unlockQualityScorePoint) float64 {
	if len(points) == 0 {
		return 0
	}
	if value <= points[0].Value {
		return points[0].Factor
	}
	for index := 1; index < len(points); index++ {
		left, right := points[index-1], points[index]
		if value <= right.Value {
			position := (value - left.Value) / (right.Value - left.Value)
			return left.Factor + position*(right.Factor-left.Factor)
		}
	}
	return points[len(points)-1].Factor
}

func unlockQualityGrade(score float64) string {
	switch {
	case score >= 90:
		return "优秀"
	case score >= 75:
		return "良好"
	case score >= 60:
		return "一般"
	default:
		return "较差"
	}
}

func rankUnlockQualityNodes(nodes []unlockQualitySnapshotNode) {
	sort.SliceStable(nodes, func(i, j int) bool {
		if nodes[i].Score == nil {
			return false
		}
		if nodes[j].Score == nil {
			return true
		}
		if *nodes[i].Score == *nodes[j].Score {
			return nodes[i].Name < nodes[j].Name
		}
		return *nodes[i].Score > *nodes[j].Score
	})
	rank := 0
	for index := range nodes {
		if nodes[index].Score == nil {
			nodes[index].Rank = nil
			continue
		}
		rank++
		value := rank
		nodes[index].Rank = &value
	}
}

func buildUnlockQualityTrend(runs []models.UnlockQualityRun, hours int) []unlockQualityTrendPoint {
	if len(runs) == 0 {
		return []unlockQualityTrendPoint{}
	}
	maxPoints := 420
	bucketSeconds := int(math.Ceil(float64(hours*3600) / float64(maxPoints)))
	if bucketSeconds < 60 {
		bucketSeconds = 60
	}
	type bucket struct {
		at       time.Time
		values   []float64
		sent     int
		failures int
	}
	buckets := make(map[int64]*bucket)
	var order []int64
	for _, run := range runs {
		key := run.FinishedAt.Unix() / int64(bucketSeconds)
		item := buckets[key]
		if item == nil {
			item = &bucket{at: time.Unix(key*int64(bucketSeconds), 0).UTC()}
			buckets[key] = item
			order = append(order, key)
		}
		item.sent += run.SamplesSent
		item.failures += run.SamplesSent - run.SamplesReceived
		if run.SamplesReceived > 0 {
			item.values = append(item.values, run.TTFBP50MS)
		}
	}
	sort.Slice(order, func(i, j int) bool { return order[i] < order[j] })
	result := make([]unlockQualityTrendPoint, 0, len(order))
	for _, key := range order {
		item := buckets[key]
		point := unlockQualityTrendPoint{
			Time: item.at, FailureCount: item.failures, SamplesSent: item.sent,
		}
		if len(item.values) > 0 {
			sorted := append([]float64(nil), item.values...)
			sort.Float64s(sorted)
			point.TTFBMinMS = roundUnlockMetric(sorted[0])
			point.TTFBMaxMS = roundUnlockMetric(sorted[len(sorted)-1])
			point.TTFBP50MS = roundUnlockMetric(unlockQualityQuantile(sorted, 0.50))
			point.TTFBP95MS = roundUnlockMetric(unlockQualityQuantile(sorted, 0.95))
		}
		result = append(result, point)
	}
	return result
}

func unlockQualityQuantile(values []float64, percentile float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sorted := append([]float64(nil), values...)
	sort.Float64s(sorted)
	position := float64(len(sorted)-1) * percentile
	lower, upper := int(math.Floor(position)), int(math.Ceil(position))
	if lower == upper {
		return sorted[lower]
	}
	fraction := position - float64(lower)
	return sorted[lower]*(1-fraction) + sorted[upper]*fraction
}

func unlockQualityStdDev(values []float64) float64 {
	if len(values) < 2 {
		return 0
	}
	var total float64
	for _, value := range values {
		total += value
	}
	average := total / float64(len(values))
	var variance float64
	for _, value := range values {
		delta := value - average
		variance += delta * delta
	}
	return math.Sqrt(variance / float64(len(values)))
}

func roundUnlockScore(value float64) float64 {
	return math.Round(value*10) / 10
}

func roundUnlockMetric(value float64) float64 {
	return math.Round(value*100) / 100
}
