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
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

func AddTCPQualityTask(task *models.TCPQualityTask) (uint, error) {
	task.SchedulePhaseMS = -1
	if _, _, err := utils.NormalizeTCPQualityTask(task); err != nil {
		return 0, err
	}
	targets, _, err := utils.GetTCPQualityTaskTargets(context.Background(), *task)
	if err != nil {
		return 0, err
	}
	if len(targets) != 1 {
		return 0, fmt.Errorf("TCP quality task must resolve to exactly one catalog target")
	}
	if err := ensureUniqueTCPQualityTarget(*task, 0); err != nil {
		return 0, err
	}
	task.ICMPTaskIDs = models.StringArray{}
	enabled := task.Enabled
	if err := dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(task).Error; err != nil {
			return err
		}
		if !enabled {
			if err := tx.Model(task).Update("enabled", false).Error; err != nil {
				return err
			}
			task.Enabled = false
		}
		pingID, err := syncManagedTCPQualityICMPTask(tx, task, targets[0], 0)
		if err != nil {
			return err
		}
		task.ICMPTaskIDs = models.StringArray{strconv.FormatUint(uint64(pingID), 10)}
		return tx.Model(task).Update("icmp_task_ids", task.ICMPTaskIDs).Error
	}); err != nil {
		return task.Id, err
	}
	if err := ReloadProbeSchedules(); err != nil {
		return task.Id, err
	}
	return task.Id, nil
}

func EditTCPQualityTask(task *models.TCPQualityTask) error {
	if task == nil || task.Id == 0 {
		return fmt.Errorf("task id is required")
	}
	existing, err := GetTCPQualityTask(task.Id)
	if err != nil {
		return err
	}
	task.ICMPTaskIDs = append(models.StringArray{}, existing.ICMPTaskIDs...)
	if task.ICMPInterval == 0 {
		task.ICMPInterval = existing.ICMPInterval
	}
	if _, _, err := utils.NormalizeTCPQualityTask(task); err != nil {
		return err
	}
	targets, _, err := utils.GetTCPQualityTaskTargets(context.Background(), *task)
	if err != nil {
		return err
	}
	if len(targets) != 1 {
		return fmt.Errorf("TCP quality task must resolve to exactly one catalog target")
	}
	if err := ensureUniqueTCPQualityTarget(*task, task.Id); err != nil {
		return err
	}
	if err := dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		updates := map[string]any{
			"name": task.Name, "clients": task.Clients, "all_clients": task.DefaultOn,
			"enabled": task.Enabled, "interval": task.Interval, "province_codes": task.ProvinceCodes,
			"isp_codes": task.ISPCode, "ip_versions": task.IPVersions,
			"standard_packets": task.StandardPackets, "large_enabled": task.LargeEnabled,
			"large_packets": task.LargePackets, "delay_ms": task.DelayMS, "timeout_ms": task.TimeoutMS,
			"icmp_interval": task.ICMPInterval, "schedule_phase_ms": -1, "schedule_interval": 0,
		}
		result := tx.Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Updates(updates)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
		currentPingID := firstTCPQualityICMPTaskID(existing.ICMPTaskIDs)
		pingID, err := syncManagedTCPQualityICMPTask(tx, task, targets[0], currentPingID)
		if err != nil {
			return err
		}
		task.ICMPTaskIDs = models.StringArray{strconv.FormatUint(uint64(pingID), 10)}
		if err := tx.Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Update("icmp_task_ids", task.ICMPTaskIDs).Error; err != nil {
			return err
		}
		if !slices.Equal(existing.ProvinceCodes, task.ProvinceCodes) || !slices.Equal(existing.ISPCode, task.ISPCode) || !slices.Equal(existing.IPVersions, task.IPVersions) {
			if err := tx.Where("task_id = ?", task.Id).Delete(&models.TCPQualityRun{}).Error; err != nil {
				return err
			}
			return tx.Where("task_id = ?", task.Id).Delete(&models.TCPQualitySnapshot{}).Error
		}
		return nil
	}); err != nil {
		return err
	}
	if err := ReloadProbeSchedules(); err != nil {
		return err
	}
	return nil
}

func DeleteTCPQualityTasks(ids []uint) error {
	if len(ids) == 0 {
		return fmt.Errorf("id is required")
	}
	db := dbcore.GetDBInstance()
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&models.PingTask{}).Where("managed_by_tcp_task IN ?", ids).Updates(map[string]any{
			"managed_by_tcp_task": 0, "catalog_target_key": "",
		}).Error; err != nil {
			return err
		}
		if err := tx.Where("task_id IN ?", ids).Delete(&models.TCPQualityRun{}).Error; err != nil {
			return err
		}
		if err := tx.Where("task_id IN ?", ids).Delete(&models.TCPQualitySnapshot{}).Error; err != nil {
			return err
		}
		result := tx.Where("id IN ?", ids).Delete(&models.TCPQualityTask{})
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
	return ReloadProbeSchedules()
}

func GetAllTCPQualityTasks() ([]models.TCPQualityTask, error) {
	var result []models.TCPQualityTask
	err := dbcore.GetDBInstance().Where("diagnostic = ?", false).Order("id ASC").Find(&result).Error
	return result, err
}

func ensureUniqueTCPQualityTarget(candidate models.TCPQualityTask, excludeID uint) error {
	taskList, err := GetAllTCPQualityTasks()
	if err != nil {
		return err
	}
	for _, existing := range taskList {
		if existing.Id == excludeID {
			continue
		}
		if slices.Equal(existing.ProvinceCodes, candidate.ProvinceCodes) &&
			slices.Equal(existing.ISPCode, candidate.ISPCode) &&
			slices.Equal(existing.IPVersions, candidate.IPVersions) {
			return fmt.Errorf("this TCP quality catalog target already has task %q", existing.Name)
		}
	}
	return nil
}

func GetTCPQualityTask(id uint) (models.TCPQualityTask, error) {
	var task models.TCPQualityTask
	err := dbcore.GetDBInstance().First(&task, "id = ?", id).Error
	return task, err
}

func GetTCPQualityTasksByClient(uuid string) []models.TCPQualityTask {
	var result []models.TCPQualityTask
	if err := dbcore.GetDBInstance().
		Where("enabled = ? AND diagnostic = ? AND clients LIKE ?", true, false, `%"`+uuid+`"%`).
		Order("id ASC").
		Find(&result).Error; err != nil {
		return nil
	}
	return result
}

func SaveTCPQualityResult(client string, params v2.TCPQualityResultParams) error {
	task, err := GetTCPQualityTask(params.TaskID)
	if err != nil {
		return err
	}
	if !task.Enabled || !task.AppliesToClient(client) {
		return fmt.Errorf("TCP quality task is not assigned to this client")
	}
	if err := validateTCPQualityResult(task, params); err != nil {
		return err
	}
	payload, err := encodeTCPQualityResults(params.Results)
	if err != nil {
		return err
	}
	finishedAt := params.FinishedAt.UTC()
	now := time.Now().UTC()
	if finishedAt.IsZero() || finishedAt.Before(now.Add(-24*time.Hour)) || finishedAt.After(now.Add(10*time.Minute)) {
		finishedAt = now
	}
	run := models.TCPQualityRun{
		TaskID:          params.TaskID,
		Client:          client,
		RunID:           params.RunID,
		CatalogRevision: params.CatalogRevision,
		Payload:         payload,
		FinishedAt:      finishedAt,
	}
	if err := dbcore.GetDBInstance().Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "task_id"}, {Name: "client"}, {Name: "run_id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"catalog_revision", "payload", "finished_at",
		}),
	}).Create(&run).Error; err != nil {
		return err
	}
	utils.CompleteTCPQualityRun(params.TaskID, client, params.RunID)
	return nil
}

func validateTCPQualityResult(task models.TCPQualityTask, params v2.TCPQualityResultParams) error {
	if params.TaskID == 0 || !validTCPQualityIdentifier(params.RunID, 64) {
		return fmt.Errorf("invalid task_id or run_id")
	}
	if !validTCPQualityIdentifier(params.CatalogRevision, 64) {
		return fmt.Errorf("invalid catalog_revision")
	}
	maxResults := len(task.ProvinceCodes) * len(task.ISPCode) * len(task.IPVersions)
	if task.LargeEnabled {
		maxResults *= 2
	}
	if len(params.Results) == 0 || len(params.Results) > maxResults {
		return fmt.Errorf("invalid TCP quality result count")
	}
	seen := make(map[string]struct{}, len(params.Results))
	for _, result := range params.Results {
		if !tcpQualityTargetAllowed(task, result.TargetKey) {
			return fmt.Errorf("target %q is not part of the task", result.TargetKey)
		}
		if result.Mode != "standard" && (result.Mode != "large" || !task.LargeEnabled) {
			return fmt.Errorf("invalid result mode %q", result.Mode)
		}
		key := result.TargetKey + ":" + result.Mode
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate result %q", key)
		}
		seen[key] = struct{}{}
		expected := task.StandardPackets
		if result.Mode == "large" {
			expected = task.LargePackets
		}
		if result.SamplesSent < 0 || result.SamplesSent > expected ||
			result.SamplesReceived < 0 || result.SamplesReceived > result.SamplesSent ||
			result.LossRatio < 0 || result.LossRatio > 1 {
			return fmt.Errorf("invalid sample counters for %q", key)
		}
		for _, value := range []float64{
			result.MinLatencyMS, result.MaxLatencyMS, result.P50LatencyMS,
			result.P95LatencyMS, result.AverageLatencyMS,
		} {
			if math.IsNaN(value) || math.IsInf(value, 0) || value < 0 || value > 120000 {
				return fmt.Errorf("invalid latency value for %q", key)
			}
		}
		if len(result.ErrorCode) > 64 || strings.ContainsAny(result.ErrorCode, "\r\n\t") {
			return fmt.Errorf("invalid error code for %q", key)
		}
	}
	return nil
}

func validTCPQualityIdentifier(value string, maxLength int) bool {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > maxLength {
		return false
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < 'A' || char > 'Z') &&
			(char < '0' || char > '9') && char != '-' && char != '_' {
			return false
		}
	}
	return true
}

func tcpQualityTargetAllowed(task models.TCPQualityTask, key string) bool {
	parts := strings.Split(key, "-")
	if len(parts) != 3 || !strings.HasPrefix(parts[2], "v") {
		return false
	}
	return containsString(task.ProvinceCodes, parts[0]) &&
		containsString(task.ISPCode, parts[1]) &&
		containsString(task.IPVersions, strings.TrimPrefix(parts[2], "v"))
}

func containsString(values []string, wanted string) bool {
	for _, value := range values {
		if value == wanted {
			return true
		}
	}
	return false
}

func encodeTCPQualityResults(results []v2.TCPQualityTargetResult) (string, error) {
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

func DecodeTCPQualityResults(payload string) ([]v2.TCPQualityTargetResult, error) {
	compressed, err := base64.RawStdEncoding.DecodeString(payload)
	if err != nil {
		return nil, err
	}
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	raw, err := io.ReadAll(io.LimitReader(reader, 8<<20))
	if err != nil {
		return nil, err
	}
	var results []v2.TCPQualityTargetResult
	if err := json.Unmarshal(raw, &results); err != nil {
		return nil, err
	}
	return results, nil
}

func GetTCPQualitySnapshot(taskID uint, windowHours int) (json.RawMessage, time.Time, error) {
	var snapshot models.TCPQualitySnapshot
	if err := dbcore.GetDBInstance().
		First(&snapshot, "task_id = ? AND window_hours = ?", taskID, windowHours).Error; err != nil {
		return nil, time.Time{}, err
	}
	return json.RawMessage(snapshot.Payload), snapshot.GeneratedAt.UTC(), nil
}

func SaveTCPQualitySnapshot(ctx context.Context, snapshot models.TCPQualitySnapshot) error {
	return dbcore.GetDBInstance().WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "task_id"}, {Name: "window_hours"}},
		DoUpdates: clause.AssignmentColumns([]string{"payload", "generated_at"}),
	}).Create(&snapshot).Error
}

func ListTCPQualityRuns(ctx context.Context, taskID uint, start time.Time) ([]models.TCPQualityRun, error) {
	var runs []models.TCPQualityRun
	err := dbcore.GetDBInstance().WithContext(ctx).
		Where("task_id = ? AND finished_at >= ?", taskID, start.UTC()).
		Order("finished_at ASC").
		Find(&runs).Error
	return runs, err
}

func ClearTCPQualityRunsBefore(before time.Time) error {
	return dbcore.GetDBInstance().Where("finished_at < ?", before.UTC()).Delete(&models.TCPQualityRun{}).Error
}

type TCPQualityDiagnosticRunView struct {
	Client     string                      `json:"client"`
	FinishedAt time.Time                   `json:"finished_at"`
	Results    []v2.TCPQualityTargetResult `json:"results"`
}

type TCPQualityDiagnosticView struct {
	ID           uint                          `json:"id"`
	Name         string                        `json:"name"`
	TargetKey    string                        `json:"target_key"`
	Province     string                        `json:"province"`
	ISP          string                        `json:"isp"`
	IPVersion    string                        `json:"ip_version"`
	Clients      []string                      `json:"clients"`
	LargeEnabled bool                          `json:"large_enabled"`
	CreatedAt    time.Time                     `json:"created_at"`
	ExpiresAt    time.Time                     `json:"expires_at"`
	Runs         []TCPQualityDiagnosticRunView `json:"runs"`
}

func RunTCPQualityCatalogDiagnostic(ctx context.Context, targetKey string, clients []string, largeEnabled bool) (uint, error) {
	target, _, err := utils.GetTCPQualityCatalogTarget(ctx, targetKey)
	if err != nil {
		return 0, err
	}
	clients = normalizeTCPQualityDiagnosticClients(clients)
	if len(clients) == 0 {
		return 0, fmt.Errorf("at least one client is required")
	}
	var count int64
	if err := dbcore.GetDBInstance().Model(&models.Client{}).Where("uuid IN ?", clients).Count(&count).Error; err != nil {
		return 0, err
	}
	if count != int64(len(clients)) {
		return 0, fmt.Errorf("diagnostic clients contain a missing node")
	}
	now := time.Now().UTC()
	expiresAt := now.Add(24 * time.Hour)
	task := models.TCPQualityTask{
		Name:    fmt.Sprintf("%s %s IPv%d 独立检测", target.Province, target.ISP, target.IPVersion),
		Clients: models.StringArray(clients), Enabled: true, Interval: 900,
		ProvinceCodes: models.StringArray{target.ProvinceCode}, ISPCode: models.StringArray{target.ISPCode},
		IPVersions: models.StringArray{strconv.Itoa(target.IPVersion)}, StandardPackets: 30,
		LargeEnabled: largeEnabled, LargePackets: 30, DelayMS: 200, TimeoutMS: 3000,
		SchedulePhaseMS: -1, Diagnostic: true, ExpiresAt: &expiresAt,
	}
	if err := dbcore.GetDBInstance().WithContext(ctx).Create(&task).Error; err != nil {
		return 0, err
	}
	if err := utils.ExecuteTCPQualityTask(ctx, task); err != nil {
		_ = dbcore.GetDBInstance().Delete(&models.TCPQualityTask{}, task.Id).Error
		return 0, err
	}
	return task.Id, nil
}

func ListTCPQualityDiagnostics(ctx context.Context) ([]TCPQualityDiagnosticView, error) {
	if err := ClearExpiredTCPQualityDiagnostics(time.Now().UTC()); err != nil {
		return nil, err
	}
	var taskList []models.TCPQualityTask
	if err := dbcore.GetDBInstance().WithContext(ctx).
		Where("diagnostic = ?", true).Order("created_at DESC").Limit(100).Find(&taskList).Error; err != nil {
		return nil, err
	}
	result := make([]TCPQualityDiagnosticView, 0, len(taskList))
	for _, task := range taskList {
		expiresAt := task.CreatedAt.UTC().Add(24 * time.Hour)
		if task.ExpiresAt != nil {
			expiresAt = task.ExpiresAt.UTC()
		}
		view := TCPQualityDiagnosticView{
			ID: task.Id, Name: task.Name, Clients: append([]string(nil), task.Clients...),
			LargeEnabled: task.LargeEnabled, CreatedAt: task.CreatedAt.UTC(), ExpiresAt: expiresAt,
			Runs: make([]TCPQualityDiagnosticRunView, 0),
		}
		if len(task.ProvinceCodes) == 1 && len(task.ISPCode) == 1 && len(task.IPVersions) == 1 {
			view.TargetKey = fmt.Sprintf("%s-%s-v%s", task.ProvinceCodes[0], task.ISPCode[0], task.IPVersions[0])
			view.IPVersion = task.IPVersions[0]
		}
		if target, _, err := utils.GetTCPQualityCatalogTarget(ctx, view.TargetKey); err == nil {
			view.Province, view.ISP = target.Province, target.ISP
		}
		runs, err := ListTCPQualityRuns(ctx, task.Id, task.CreatedAt.Add(-time.Minute))
		if err != nil {
			return nil, err
		}
		for _, run := range runs {
			decoded, err := DecodeTCPQualityResults(run.Payload)
			if err != nil {
				continue
			}
			view.Runs = append(view.Runs, TCPQualityDiagnosticRunView{
				Client: run.Client, FinishedAt: run.FinishedAt.UTC(), Results: decoded,
			})
		}
		result = append(result, view)
	}
	return result, nil
}

func ClearExpiredTCPQualityDiagnostics(now time.Time) error {
	db := dbcore.GetDBInstance()
	return db.Transaction(func(tx *gorm.DB) error {
		var ids []uint
		if err := tx.Model(&models.TCPQualityTask{}).
			Where("diagnostic = ? AND expires_at IS NOT NULL AND expires_at <= ?", true, now.UTC()).
			Pluck("id", &ids).Error; err != nil {
			return err
		}
		if len(ids) == 0 {
			return nil
		}
		if err := tx.Where("task_id IN ?", ids).Delete(&models.TCPQualityRun{}).Error; err != nil {
			return err
		}
		if err := tx.Where("task_id IN ?", ids).Delete(&models.TCPQualitySnapshot{}).Error; err != nil {
			return err
		}
		return tx.Where("id IN ?", ids).Delete(&models.TCPQualityTask{}).Error
	})
}

func normalizeTCPQualityDiagnosticClients(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
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
		if len(result) == 100 {
			break
		}
	}
	return result
}

func validateTCPQualityICMPTasks(ids []string) error {
	if len(ids) == 0 {
		return nil
	}
	parsed := make([]uint, 0, len(ids))
	for _, value := range ids {
		id, err := strconv.ParseUint(value, 10, 32)
		if err != nil || id == 0 {
			return fmt.Errorf("invalid ICMP task id %q", value)
		}
		parsed = append(parsed, uint(id))
	}
	var count int64
	if err := dbcore.GetDBInstance().Model(&models.PingTask{}).
		Where("id IN ? AND type = ?", parsed, "icmp").Count(&count).Error; err != nil {
		return err
	}
	if count != int64(len(parsed)) {
		return fmt.Errorf("icmp_task_ids contains a missing or non-ICMP task")
	}
	return nil
}

func firstTCPQualityICMPTaskID(ids []string) uint {
	for _, raw := range ids {
		parsed, err := strconv.ParseUint(strings.TrimSpace(raw), 10, 64)
		if err == nil && parsed > 0 {
			return uint(parsed)
		}
	}
	return 0
}

func syncManagedTCPQualityICMPTask(tx *gorm.DB, task *models.TCPQualityTask, target v2.TCPQualityTarget, preferredID uint) (uint, error) {
	if task == nil || task.Id == 0 {
		return 0, fmt.Errorf("TCP quality task must be saved before binding ICMP")
	}
	var pingTask models.PingTask
	if preferredID > 0 {
		if err := tx.First(&pingTask, "id = ?", preferredID).Error; err != nil && err != gorm.ErrRecordNotFound {
			return 0, err
		}
		managedByCurrentTask := pingTask.Id > 0 && pingTask.ManagedByTCPTask == task.Id && pingTask.Type == "icmp"
		if pingTask.Id > 0 && !managedByCurrentTask && (pingTask.Type != "icmp" || strings.TrimSpace(pingTask.Target) != strings.TrimSpace(target.Address)) {
			pingTask = models.PingTask{}
		}
	}
	if pingTask.Id == 0 {
		err := tx.Where("type = ? AND target = ? AND name = ? AND managed_by_tcp_task IN ?", "icmp", target.Address, task.Name, []uint{0, task.Id}).
			Order("id ASC").First(&pingTask).Error
		if err != nil && err != gorm.ErrRecordNotFound {
			return 0, err
		}
	}
	if pingTask.Id == 0 {
		pingTask = models.PingTask{
			Name: task.Name, Clients: append(models.StringArray{}, task.Clients...), DefaultOn: task.DefaultOn,
			Type: "icmp", Target: target.Address, Interval: task.ICMPInterval,
			ProbeConfig:     models.ProbeConfig{SampleCount: 1, TimeoutMS: 3000, PreferredIP: strconv.Itoa(target.IPVersion)},
			SchedulePhaseMS: -1, ManagedByTCPTask: task.Id, CatalogTargetKey: target.Key,
		}
		if err := tx.Create(&pingTask).Error; err != nil {
			return 0, err
		}
		if err := tx.Model(&models.PingTask{}).Where("id = ?", pingTask.Id).Update("weight", int(pingTask.Id)).Error; err != nil {
			return 0, err
		}
		return pingTask.Id, nil
	}

	config := pingTask.ProbeConfig
	if config.SampleCount < 1 {
		config.SampleCount = 1
	}
	if config.TimeoutMS < 1 {
		config.TimeoutMS = 3000
	}
	config.PreferredIP = strconv.Itoa(target.IPVersion)
	updates := map[string]any{
		"name": task.Name, "clients": task.Clients, "all_clients": task.DefaultOn,
		"type": "icmp", "target": target.Address, "interval": task.ICMPInterval,
		"probe_config": config, "managed_by_tcp_task": task.Id, "catalog_target_key": target.Key,
		"schedule_phase_ms": -1, "schedule_interval": 0,
	}
	if err := tx.Model(&models.PingTask{}).Where("id = ?", pingTask.Id).Updates(updates).Error; err != nil {
		return 0, err
	}
	return pingTask.Id, nil
}

// SyncTCPQualityICMPTargets follows the last known-good catalog. Concrete
// endpoint changes update only the private managed PingTask; public APIs still
// expose the stable task label without an address.
func SyncTCPQualityICMPTargets(ctx context.Context) error {
	taskList, err := GetAllTCPQualityTasks()
	if err != nil {
		return err
	}
	return dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		for index := range taskList {
			task := &taskList[index]
			if task.ICMPInterval <= 0 {
				task.ICMPInterval = 60
				if pingID := firstTCPQualityICMPTaskID(task.ICMPTaskIDs); pingID > 0 {
					var linked models.PingTask
					if err := tx.First(&linked, "id = ?", pingID).Error; err == nil && linked.Interval > 0 {
						task.ICMPInterval = linked.Interval
					}
				}
			}
			targets, _, err := utils.GetTCPQualityTaskTargets(ctx, *task)
			if err != nil {
				return err
			}
			if len(targets) != 1 {
				// Legacy versions allowed a disabled task to contain several
				// catalog targets. Keep that historical configuration inert until
				// the administrator deletes it or edits it into the new one-target
				// model; enabled tasks must always be unambiguous.
				if !task.Enabled {
					continue
				}
				return fmt.Errorf("TCP quality task %d must resolve to exactly one catalog target", task.Id)
			}
			pingID, err := syncManagedTCPQualityICMPTask(tx, task, targets[0], firstTCPQualityICMPTaskID(task.ICMPTaskIDs))
			if err != nil {
				return err
			}
			task.ICMPTaskIDs = models.StringArray{strconv.FormatUint(uint64(pingID), 10)}
			if err := tx.Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Updates(map[string]any{
				"icmp_task_ids": task.ICMPTaskIDs, "icmp_interval": task.ICMPInterval,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func ReloadTCPQualitySchedule() error {
	return ReloadProbeSchedules()
}

func RunTCPQualityTaskNow(ctx context.Context, id uint) error {
	task, err := GetTCPQualityTask(id)
	if err != nil {
		return err
	}
	return utils.ExecuteTCPQualityTask(ctx, task)
}

func AddDefaultTCPQualityClientUUID(uuid string) error {
	if strings.TrimSpace(uuid) == "" {
		return nil
	}
	var taskList []models.TCPQualityTask
	db := dbcore.GetDBInstance()
	if err := db.Where("all_clients = ? AND diagnostic = ?", true, false).Find(&taskList).Error; err != nil {
		return err
	}
	changed := false
	for _, task := range taskList {
		if containsString(task.Clients, uuid) {
			continue
		}
		next := append(models.StringArray{}, task.Clients...)
		next = append(next, uuid)
		if err := db.Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Updates(map[string]any{
			"clients": next, "schedule_phase_ms": -1, "schedule_interval": 0,
		}).Error; err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return ReloadProbeSchedules()
	}
	return nil
}
