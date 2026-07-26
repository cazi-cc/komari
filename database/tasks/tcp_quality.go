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
	if _, _, err := utils.NormalizeTCPQualityTask(task); err != nil {
		return 0, err
	}
	if err := validateTCPQualityICMPTasks(task.ICMPTaskIDs); err != nil {
		return 0, err
	}
	enabled := task.Enabled
	if err := dbcore.GetDBInstance().Create(task).Error; err != nil {
		return 0, err
	}
	if !enabled {
		if err := dbcore.GetDBInstance().Model(task).Update("enabled", false).Error; err != nil {
			return task.Id, err
		}
		task.Enabled = false
	}
	if err := ReloadTCPQualitySchedule(); err != nil {
		return task.Id, err
	}
	return task.Id, nil
}

func EditTCPQualityTask(task *models.TCPQualityTask) error {
	if task == nil || task.Id == 0 {
		return fmt.Errorf("task id is required")
	}
	if _, _, err := utils.NormalizeTCPQualityTask(task); err != nil {
		return err
	}
	if err := validateTCPQualityICMPTasks(task.ICMPTaskIDs); err != nil {
		return err
	}
	updates := map[string]any{
		"name":             task.Name,
		"clients":          task.Clients,
		"all_clients":      task.DefaultOn,
		"enabled":          task.Enabled,
		"interval":         task.Interval,
		"province_codes":   task.ProvinceCodes,
		"isp_codes":        task.ISPCode,
		"ip_versions":      task.IPVersions,
		"icmp_task_ids":    task.ICMPTaskIDs,
		"standard_packets": task.StandardPackets,
		"large_enabled":    task.LargeEnabled,
		"large_packets":    task.LargePackets,
		"delay_ms":         task.DelayMS,
		"timeout_ms":       task.TimeoutMS,
	}
	result := dbcore.GetDBInstance().Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	if err := ReloadTCPQualitySchedule(); err != nil {
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
	return ReloadTCPQualitySchedule()
}

func GetAllTCPQualityTasks() ([]models.TCPQualityTask, error) {
	var result []models.TCPQualityTask
	err := dbcore.GetDBInstance().Order("id ASC").Find(&result).Error
	return result, err
}

func GetTCPQualityTask(id uint) (models.TCPQualityTask, error) {
	var task models.TCPQualityTask
	err := dbcore.GetDBInstance().First(&task, "id = ?", id).Error
	return task, err
}

func GetTCPQualityTasksByClient(uuid string) []models.TCPQualityTask {
	var result []models.TCPQualityTask
	if err := dbcore.GetDBInstance().
		Where("enabled = ? AND clients LIKE ?", true, `%"`+uuid+`"%`).
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

func ReloadTCPQualitySchedule() error {
	tasks, err := GetAllTCPQualityTasks()
	if err != nil {
		return err
	}
	return utils.ReloadTCPQualitySchedule(tasks)
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
		if err := db.Model(&models.TCPQualityTask{}).Where("id = ?", task.Id).Update("clients", next).Error; err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return ReloadTCPQualitySchedule()
	}
	return nil
}
