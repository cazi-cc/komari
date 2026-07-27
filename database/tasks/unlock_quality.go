package tasks

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/utils/messageSender"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const unlockQualityCatalogRevision = "chatgpt_v1"

func NormalizeUnlockQualityTask(task *models.UnlockQualityTask) error {
	if task == nil {
		return fmt.Errorf("task is required")
	}
	task.Name = strings.TrimSpace(task.Name)
	if task.Name == "" || len(task.Name) > 255 || strings.ContainsAny(task.Name, "\r\n\t") {
		return fmt.Errorf("name must contain 1 to 255 printable characters")
	}
	task.Service = strings.ToLower(strings.TrimSpace(task.Service))
	if task.Service == "" {
		task.Service = "chatgpt"
	}
	if task.Service != "chatgpt" {
		return fmt.Errorf("only ChatGPT is available in this release")
	}
	if task.Interval < 60 || task.Interval > 86400 {
		return fmt.Errorf("interval must be between 60 and 86400 seconds")
	}
	if task.VerifyInterval < 300 || task.VerifyInterval > 86400 || task.VerifyInterval < task.Interval {
		return fmt.Errorf("verify_interval must be between 300 and 86400 seconds and not shorter than interval")
	}
	if task.SampleCount < 1 || task.SampleCount > 3 {
		return fmt.Errorf("sample_count must be between 1 and 3")
	}
	if task.TimeoutMS < 1000 || task.TimeoutMS > 30000 {
		return fmt.Errorf("timeout_ms must be between 1000 and 30000")
	}
	task.Clients = uniqueUnlockQualityStrings(task.Clients)
	if len(task.Clients) == 0 {
		return fmt.Errorf("at least one client is required")
	}
	task.ControlDNS = strings.TrimSpace(task.ControlDNS)
	task.FixedAddress = strings.TrimSpace(task.FixedAddress)
	if task.ControlEnabled && !validUnlockQualityDNSServer(task.ControlDNS) {
		return fmt.Errorf("control_dns must be an IP address with an optional port")
	}
	if task.FixedEnabled && net.ParseIP(task.FixedAddress) == nil {
		return fmt.Errorf("fixed_address must be an IP address")
	}
	if len(task.ControlDNS) > 255 || len(task.FixedAddress) > 255 ||
		strings.ContainsAny(task.ControlDNS+task.FixedAddress, "\r\n\t") {
		return fmt.Errorf("route configuration is invalid")
	}
	return nil
}

func validUnlockQualityDNSServer(value string) bool {
	value = strings.TrimSpace(value)
	if net.ParseIP(value) != nil {
		return true
	}
	host, port, err := net.SplitHostPort(value)
	if err != nil || net.ParseIP(strings.Trim(host, "[]")) == nil {
		return false
	}
	parsedPort, err := net.LookupPort("udp", port)
	return err == nil && parsedPort > 0 && parsedPort <= 65535
}

func uniqueUnlockQualityStrings(values []string) models.StringArray {
	result := make(models.StringArray, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

func AddUnlockQualityTask(task *models.UnlockQualityTask) (uint, error) {
	if err := NormalizeUnlockQualityTask(task); err != nil {
		return 0, err
	}
	if err := dbcore.GetDBInstance().Create(task).Error; err != nil {
		return 0, err
	}
	updates := map[string]any{
		"all_clients":           task.DefaultOn,
		"enabled":               task.Enabled,
		"control_enabled":       task.ControlEnabled,
		"fixed_enabled":         task.FixedEnabled,
		"notifications_enabled": task.NotificationsEnabled,
	}
	if err := dbcore.GetDBInstance().Model(task).Updates(updates).Error; err != nil {
		return task.Id, err
	}
	if err := ReloadUnlockQualitySchedule(); err != nil {
		return task.Id, err
	}
	return task.Id, nil
}

func EditUnlockQualityTask(task *models.UnlockQualityTask) error {
	if task == nil || task.Id == 0 {
		return fmt.Errorf("task id is required")
	}
	if err := NormalizeUnlockQualityTask(task); err != nil {
		return err
	}
	updates := map[string]any{
		"name":                  task.Name,
		"clients":               task.Clients,
		"all_clients":           task.DefaultOn,
		"enabled":               task.Enabled,
		"service":               task.Service,
		"interval":              task.Interval,
		"verify_interval":       task.VerifyInterval,
		"sample_count":          task.SampleCount,
		"timeout_ms":            task.TimeoutMS,
		"control_enabled":       task.ControlEnabled,
		"control_dns":           task.ControlDNS,
		"fixed_enabled":         task.FixedEnabled,
		"fixed_address":         task.FixedAddress,
		"notifications_enabled": task.NotificationsEnabled,
	}
	result := dbcore.GetDBInstance().Model(&models.UnlockQualityTask{}).Where("id = ?", task.Id).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	return ReloadUnlockQualitySchedule()
}

func DeleteUnlockQualityTasks(ids []uint) error {
	if len(ids) == 0 {
		return fmt.Errorf("id is required")
	}
	db := dbcore.GetDBInstance()
	if err := db.Transaction(func(tx *gorm.DB) error {
		for _, model := range []any{
			&models.UnlockQualityRun{},
			&models.UnlockQualitySnapshot{},
			&models.UnlockQualityAlertState{},
		} {
			if err := tx.Where("task_id IN ?", ids).Delete(model).Error; err != nil {
				return err
			}
		}
		result := tx.Where("id IN ?", ids).Delete(&models.UnlockQualityTask{})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		return nil
	}); err != nil {
		return err
	}
	return ReloadUnlockQualitySchedule()
}

func GetAllUnlockQualityTasks() ([]models.UnlockQualityTask, error) {
	var result []models.UnlockQualityTask
	err := dbcore.GetDBInstance().Order("id ASC").Find(&result).Error
	return result, err
}

func GetUnlockQualityTask(id uint) (models.UnlockQualityTask, error) {
	var task models.UnlockQualityTask
	err := dbcore.GetDBInstance().First(&task, "id = ?", id).Error
	return task, err
}

func SaveUnlockQualityResult(client string, params v2.UnlockQualityResultParams) error {
	task, err := GetUnlockQualityTask(params.TaskID)
	if err != nil {
		return err
	}
	if !task.Enabled || !task.AppliesToClient(client) {
		return fmt.Errorf("unlock quality task is not assigned to this client")
	}
	if err := validateUnlockQualityResult(task, params); err != nil {
		return err
	}
	payload, err := encodeUnlockQualityResults(params.Results)
	if err != nil {
		return err
	}
	webResult, traceResult := findUnlockQualityResults(params.Results)
	if webResult == nil {
		return fmt.Errorf("web result is required")
	}
	finishedAt := params.FinishedAt.UTC()
	now := time.Now().UTC()
	if finishedAt.IsZero() || finishedAt.Before(now.Add(-24*time.Hour)) || finishedAt.After(now.Add(10*time.Minute)) {
		finishedAt = now
	}
	run := models.UnlockQualityRun{
		TaskID:                params.TaskID,
		Client:                client,
		RunID:                 params.RunID,
		Service:               params.Service,
		CatalogRevision:       params.CatalogRevision,
		RouteMode:             params.RouteMode,
		ProbeKind:             params.ProbeKind,
		Verdict:               params.Verdict,
		SamplesSent:           webResult.SamplesSent,
		SamplesReceived:       webResult.SamplesReceived,
		FailureRatio:          webResult.FailureRatio,
		DNSMS:                 webResult.DNSMS,
		ConnectMS:             webResult.ConnectMS,
		TLSMS:                 webResult.TLSMS,
		TTFBP50MS:             webResult.TTFBP50MS,
		TTFBP95MS:             webResult.TTFBP95MS,
		TotalP50MS:            webResult.TotalP50MS,
		TotalP95MS:            webResult.TotalP95MS,
		JitterMS:              webResult.JitterMS,
		HTTPStatusCode:        webResult.HTTPStatusCode,
		HTTPStatusOKRatio:     webResult.HTTPStatusOKRatio,
		TCPRetransmissions:    webResult.TCPRetransmissions,
		ResolvedAddressHash:   webResult.ResolvedAddressHash,
		ResolvedAddressFamily: webResult.ResolvedAddressFamily,
		Payload:               payload,
		FinishedAt:            finishedAt,
	}
	if traceResult != nil {
		run.ExitCountry = traceResult.ExitCountry
		run.EdgeColo = traceResult.EdgeColo
	}
	if err := dbcore.GetDBInstance().Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "task_id"}, {Name: "client"}, {Name: "run_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"service", "catalog_revision", "route_mode", "probe_kind", "verdict",
			"samples_sent", "samples_received", "failure_ratio", "dns_ms", "connect_ms",
			"tls_ms", "ttfb_p50_ms", "ttfb_p95_ms", "total_p50_ms", "total_p95_ms",
			"jitter_ms", "http_status_code", "http_status_ok_ratio", "tcp_retransmissions",
			"resolved_address_hash", "resolved_address_family", "exit_country", "edge_colo",
			"payload", "finished_at",
		}),
	}).Create(&run).Error; err != nil {
		return err
	}
	utils.CompleteUnlockQualityRun(params.TaskID, client, params.RouteMode, params.RunID)
	if task.NotificationsEnabled && params.RouteMode == "system" && params.ProbeKind == "verify" {
		go updateUnlockQualityAlert(task, client, params.Verdict, finishedAt)
	}
	return nil
}

func validateUnlockQualityResult(task models.UnlockQualityTask, params v2.UnlockQualityResultParams) error {
	if params.TaskID == 0 || !validTCPQualityIdentifier(params.RunID, 64) ||
		params.Service != task.Service || params.CatalogRevision != unlockQualityCatalogRevision {
		return fmt.Errorf("invalid unlock quality identity")
	}
	if params.RouteMode != "system" &&
		(params.RouteMode != "control" || !task.ControlEnabled) &&
		(params.RouteMode != "fixed" || !task.FixedEnabled) {
		return fmt.Errorf("route mode is not enabled for this task")
	}
	if params.ProbeKind != "minute" && params.ProbeKind != "verify" {
		return fmt.Errorf("invalid probe kind")
	}
	if !validUnlockQualityVerdict(params.Verdict) {
		return fmt.Errorf("invalid verdict")
	}
	maxResults := 1
	if params.ProbeKind == "verify" {
		maxResults = 5
	}
	if len(params.Results) < 1 || len(params.Results) > maxResults {
		return fmt.Errorf("invalid endpoint result count")
	}
	allowed := map[string]bool{"web": true, "auth": true, "api": true, "static": true, "trace": true}
	seen := make(map[string]struct{}, len(params.Results))
	for _, result := range params.Results {
		if !allowed[result.EndpointKey] {
			return fmt.Errorf("invalid endpoint key")
		}
		if _, exists := seen[result.EndpointKey]; exists {
			return fmt.Errorf("duplicate endpoint result")
		}
		seen[result.EndpointKey] = struct{}{}
		if result.SamplesSent < 1 || result.SamplesSent > task.SampleCount ||
			result.SamplesReceived < 0 || result.SamplesReceived > result.SamplesSent ||
			result.FailureRatio < 0 || result.FailureRatio > 1 ||
			result.HTTPStatusOKRatio < 0 || result.HTTPStatusOKRatio > 1 {
			return fmt.Errorf("invalid endpoint counters")
		}
		for _, value := range []float64{
			result.DNSMS, result.ConnectMS, result.TLSMS, result.TTFBP50MS,
			result.TTFBP95MS, result.TotalP50MS, result.TotalP95MS, result.JitterMS,
		} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 120000 {
				return fmt.Errorf("invalid endpoint timing")
			}
		}
		if !validUnlockQualityVerdict(result.Verdict) ||
			len(result.ErrorCode) > 64 || strings.ContainsAny(result.ErrorCode, "\r\n\t") {
			return fmt.Errorf("invalid endpoint status")
		}
		if len(result.ResolvedAddressHash) > 64 || len(result.ResolvedAddressFamily) > 16 ||
			!validUnlockLocation(result.ExitCountry, 2) || !validUnlockLocation(result.EdgeColo, 3) {
			return fmt.Errorf("invalid endpoint metadata")
		}
	}
	return nil
}

func validUnlockQualityVerdict(value string) bool {
	switch value {
	case "available", "partial", "region_limited", "unavailable":
		return true
	default:
		return false
	}
}

func validUnlockLocation(value string, length int) bool {
	if value == "" {
		return true
	}
	if len(value) != length {
		return false
	}
	for _, char := range value {
		if char < 'A' || char > 'Z' {
			return false
		}
	}
	return true
}

func findUnlockQualityResults(results []v2.UnlockQualityEndpointResult) (*v2.UnlockQualityEndpointResult, *v2.UnlockQualityEndpointResult) {
	var webResult, traceResult *v2.UnlockQualityEndpointResult
	for index := range results {
		switch results[index].EndpointKey {
		case "web":
			webResult = &results[index]
		case "trace":
			traceResult = &results[index]
		}
	}
	return webResult, traceResult
}

func encodeUnlockQualityResults(results []v2.UnlockQualityEndpointResult) (string, error) {
	raw, err := json.Marshal(results)
	if err != nil {
		return "", err
	}
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(raw); err != nil {
		return "", err
	}
	if err := writer.Close(); err != nil {
		return "", err
	}
	return base64.RawStdEncoding.EncodeToString(compressed.Bytes()), nil
}

func DecodeUnlockQualityResults(payload string) ([]v2.UnlockQualityEndpointResult, error) {
	compressed, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 1<<20))
	if err != nil {
		return nil, err
	}
	var result []v2.UnlockQualityEndpointResult
	if err := json.Unmarshal(raw, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func GetUnlockQualitySnapshot(taskID uint, windowHours int) (json.RawMessage, error) {
	var snapshot models.UnlockQualitySnapshot
	if err := dbcore.GetDBInstance().
		First(&snapshot, "task_id = ? AND window_hours = ?", taskID, windowHours).Error; err != nil {
		return nil, err
	}
	return json.RawMessage(snapshot.Payload), nil
}

func SaveUnlockQualitySnapshot(ctx context.Context, snapshot models.UnlockQualitySnapshot) error {
	return dbcore.GetDBInstance().WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_id"}, {Name: "window_hours"}},
		DoUpdates: clause.AssignmentColumns([]string{"payload", "generated_at"}),
	}).Create(&snapshot).Error
}

func ListUnlockQualityRuns(ctx context.Context, taskID uint, start time.Time) ([]models.UnlockQualityRun, error) {
	var runs []models.UnlockQualityRun
	err := dbcore.GetDBInstance().WithContext(ctx).
		Select(
			"task_id", "client", "route_mode", "probe_kind", "verdict",
			"samples_sent", "samples_received", "failure_ratio",
			"dns_ms", "connect_ms", "tls_ms", "ttfb_p50_ms", "ttfb_p95_ms",
			"total_p50_ms", "total_p95_ms", "jitter_ms", "http_status_ok_ratio",
			"tcp_retransmissions", "exit_country", "edge_colo", "finished_at",
		).
		Where("task_id = ? AND finished_at >= ?", taskID, start.UTC()).
		Order("finished_at ASC").
		Find(&runs).Error
	return runs, err
}

func ClearUnlockQualityRunsBefore(before time.Time) error {
	return dbcore.GetDBInstance().Where("finished_at < ?", before.UTC()).Delete(&models.UnlockQualityRun{}).Error
}

func ReloadUnlockQualitySchedule() error {
	taskList, err := GetAllUnlockQualityTasks()
	if err != nil {
		return err
	}
	return utils.ReloadUnlockQualitySchedule(taskList)
}

func RunUnlockQualityTaskNow(ctx context.Context, id uint) error {
	task, err := GetUnlockQualityTask(id)
	if err != nil {
		return err
	}
	return utils.ExecuteUnlockQualityTask(ctx, task, true)
}

func AddDefaultUnlockQualityClientUUID(uuid string) error {
	if strings.TrimSpace(uuid) == "" {
		return nil
	}
	var taskList []models.UnlockQualityTask
	db := dbcore.GetDBInstance()
	if err := db.Where("all_clients = ?", true).Find(&taskList).Error; err != nil {
		return err
	}
	changed := false
	for _, task := range taskList {
		if containsString(task.Clients, uuid) {
			continue
		}
		next := append(models.StringArray{}, task.Clients...)
		next = append(next, uuid)
		if err := db.Model(&models.UnlockQualityTask{}).Where("id = ?", task.Id).Update("clients", next).Error; err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return ReloadUnlockQualitySchedule()
	}
	return nil
}

func updateUnlockQualityAlert(task models.UnlockQualityTask, clientUUID, verdict string, observedAt time.Time) {
	db := dbcore.GetDBInstance()
	state := models.UnlockQualityAlertState{TaskID: task.Id, Client: clientUUID, RouteMode: "system"}
	_ = db.FirstOrCreate(&state, "task_id = ? AND client = ? AND route_mode = ?", task.Id, clientUUID, "system").Error
	if unlockQualityVerdictAbnormal(state.LastObserved) == unlockQualityVerdictAbnormal(verdict) {
		state.Consecutive++
	} else {
		state.Consecutive = 1
	}
	state.LastObserved = verdict
	if state.Consecutive < 2 {
		_ = db.Save(&state).Error
		return
	}

	abnormal := verdict != "available"
	notify := abnormal != state.AlertActive
	state.AlertActive = abnormal
	if notify {
		at := observedAt.UTC()
		state.LastNotifiedAt = &at
	}
	if err := db.Save(&state).Error; err != nil || !notify {
		return
	}

	var client models.Client
	if err := db.Select("uuid", "name").First(&client, "uuid = ?", clientUUID).Error; err != nil {
		client.UUID = clientUUID
		client.Name = clientUUID
	}
	status := unlockQualityVerdictLabel(verdict)
	title := "Komari ChatGPT 线路恢复"
	message := fmt.Sprintf("%s 的 %s 已连续两轮恢复正常。", client.Name, task.Name)
	if abnormal {
		title = "Komari ChatGPT 线路异常"
		message = fmt.Sprintf("%s 的 %s 已连续两轮异常，当前状态：%s。", client.Name, task.Name, status)
	}
	_ = messageSender.SendTextMessage(message, title)
}

func unlockQualityVerdictAbnormal(verdict string) bool {
	return verdict != "" && verdict != "available"
}

func unlockQualityVerdictLabel(verdict string) string {
	switch verdict {
	case "available":
		return "可用"
	case "partial":
		return "部分可用"
	case "region_limited":
		return "地区受限"
	default:
		return "不可用"
	}
}
