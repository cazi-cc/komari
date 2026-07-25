package tasks

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/metricstore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
)

// AddPingTask 创建延迟监测任务。defaultOn 表示新加入的服务器是否自动开启此监测。
func AddPingTask(clients []string, defaultOn bool, name string, target, taskType string, interval int, probeConfig models.ProbeConfig) (uint, error) {
	db := dbcore.GetDBInstance()
	normalizedClients := normalizePingClients(models.StringArray(clients))
	normalizedConfig, err := normalizeProbeConfig(taskType, interval, probeConfig)
	if err != nil {
		return 0, err
	}
	task := models.PingTask{
		Clients:     normalizedClients,
		DefaultOn:   defaultOn,
		Name:        name,
		Type:        taskType,
		Target:      target,
		Interval:    interval,
		ProbeConfig: normalizedConfig,
	}
	err = db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&task).Error; err != nil {
			return err
		}

		// Append by id to avoid races between concurrent create requests.
		result := tx.Model(&models.PingTask{}).Where("id = ?", task.Id).Update("weight", int(task.Id))
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}

		return nil
	})
	if err != nil {
		return 0, err
	}
	ReloadPingSchedule()
	return task.Id, nil
}

func DeletePingTask(id []uint) error {
	// The metric store is independent from the main database, so clean it first
	// to avoid leaving history that can no longer be addressed through the task.
	if err := DeletePingRecords(id); err != nil {
		return err
	}

	db := dbcore.GetDBInstance()
	result := db.Where("id IN ?", id).Delete(&models.PingTask{})
	if result.RowsAffected == 0 {
		return gorm.ErrRecordNotFound
	}
	ReloadPingSchedule()
	return result.Error
}

// EditPingTask 批量更新延迟监测任务配置。
func EditPingTask(tasks []*models.PingTask) error {
	db := dbcore.GetDBInstance()
	for _, task := range tasks {
		task.Clients = normalizePingClients(task.Clients)
		normalizedConfig, err := normalizeProbeConfig(task.Type, task.Interval, task.ProbeConfig)
		if err != nil {
			return err
		}
		task.ProbeConfig = normalizedConfig
		// 使用 map 显式更新，避免 GORM struct Updates 跳过 false/0/空切片等零值。
		updates := map[string]interface{}{
			"name":         task.Name,
			"clients":      task.Clients,
			"all_clients":  task.DefaultOn,
			"type":         task.Type,
			"target":       task.Target,
			"interval":     task.Interval,
			"probe_config": task.ProbeConfig,
		}
		result := db.Model(&models.PingTask{}).Where("id = ?", task.Id).Updates(updates)
		if result.RowsAffected == 0 {
			return gorm.ErrRecordNotFound
		}
	}
	ReloadPingSchedule()
	return nil
}

func normalizeProbeConfig(taskType string, interval int, config models.ProbeConfig) (models.ProbeConfig, error) {
	switch taskType {
	case "icmp", "tcp", "http":
	default:
		return models.ProbeConfig{}, fmt.Errorf("unsupported ping task type %q", taskType)
	}
	if interval <= 0 {
		return models.ProbeConfig{}, fmt.Errorf("interval must be positive")
	}
	if config.SampleCount < 0 || config.SampleCount > 10 {
		return models.ProbeConfig{}, fmt.Errorf("sample_count must be between 1 and 10")
	}
	if config.SampleCount > 1 && interval < 5 {
		return models.ProbeConfig{}, fmt.Errorf("multi-sample probes require an interval of at least 5 seconds")
	}
	if config.TimeoutMS < 0 || config.TimeoutMS > 10000 {
		return models.ProbeConfig{}, fmt.Errorf("timeout_ms must be between 1 and 10000")
	}
	if taskType == "icmp" && config.PacketSize != 0 && (config.PacketSize < 24 || config.PacketSize > 1400) {
		return models.ProbeConfig{}, fmt.Errorf("packet_size must be between 24 and 1400")
	}
	if taskType != "icmp" {
		config.PacketSize = 0
	}
	config.DNSServer = strings.TrimSpace(config.DNSServer)
	if len(config.DNSServer) > 255 || strings.ContainsAny(config.DNSServer, "\r\n\t") {
		return models.ProbeConfig{}, fmt.Errorf("dns_server is invalid")
	}
	config.PreferredIP = strings.TrimSpace(config.PreferredIP)
	if config.PreferredIP != "" && config.PreferredIP != "4" && config.PreferredIP != "6" {
		return models.ProbeConfig{}, fmt.Errorf("preferred_ip must be empty, 4 or 6")
	}
	if len(config.ValidStatusCodes) > 100 {
		return models.ProbeConfig{}, fmt.Errorf("valid_status_codes cannot contain more than 100 values")
	}
	seenStatuses := make(map[int]struct{}, len(config.ValidStatusCodes))
	statuses := make([]int, 0, len(config.ValidStatusCodes))
	for _, status := range config.ValidStatusCodes {
		if status < 100 || status > 599 {
			return models.ProbeConfig{}, fmt.Errorf("invalid HTTP status code %d", status)
		}
		if _, exists := seenStatuses[status]; exists {
			continue
		}
		seenStatuses[status] = struct{}{}
		statuses = append(statuses, status)
	}
	config.ValidStatusCodes = statuses
	return config, nil
}

// normalizePingClients 保持 clients 字段序列化为 JSON 数组，避免空值变成 null。
func normalizePingClients(clients models.StringArray) models.StringArray {
	if clients == nil {
		return models.StringArray{}
	}
	return clients
}

func GetAllPingTasks() ([]models.PingTask, error) {
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Order("weight ASC").Order("id ASC").Find(&tasks).Error; err != nil {
		return nil, err
	}
	return tasks, nil
}

// GetPingTasksByClient 获取指定服务器需要执行的延迟监测任务。
func GetPingTasksByClient(uuid string) []models.PingTask {
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Where("clients LIKE ?", `%"`+uuid+`"%`).Find(&tasks).Error; err != nil {
		return nil
	}
	return tasks
}

func UpdatePingTaskOrder(order map[uint]int) error {
	db := dbcore.GetDBInstance()
	err := db.Transaction(func(tx *gorm.DB) error {
		for id, weight := range order {
			result := tx.Model(&models.PingTask{}).Where("id = ?", id).Update("weight", weight)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return gorm.ErrRecordNotFound
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	ReloadPingSchedule()
	return nil
}

// ping 记录已完全迁移到 metric store（指标 ping.latency_ms），运行期读写全部走
// metric store，旧 ping_records 表不再参与。

func SavePingRecord(record models.PingRecord) error {
	return metricstore.WritePingRecord(context.Background(), record)
}

func DeletePingRecords(id []uint) error {
	return metricstore.DeletePingRecordsByTask(context.Background(), id)
}

func DeleteAllPingRecords() error {
	return metricstore.DeleteAllPingRecords(context.Background())
}

func ReloadPingSchedule() error {
	pingTasks, err := GetAllPingTasks()
	if err != nil {
		return err
	}
	return utils.ReloadPingSchedule(pingTasks)
}

// AddDefaultOnClientUUID 在新客户端注册后，把该 UUID 追加到所有 default_on=true 的任务的 clients 中（去重）。
func AddDefaultOnClientUUID(uuid string) error {
	if uuid == "" {
		return nil
	}
	db := dbcore.GetDBInstance()
	var tasks []models.PingTask
	if err := db.Where("all_clients = ?", true).Find(&tasks).Error; err != nil {
		return err
	}
	if len(tasks) == 0 {
		return nil
	}
	changed := false
	for _, task := range tasks {
		exists := false
		for _, c := range task.Clients {
			if c == uuid {
				exists = true
				break
			}
		}
		if exists {
			continue
		}
		next := append(models.StringArray{}, task.Clients...)
		next = append(next, uuid)
		if err := db.Model(&models.PingTask{}).Where("id = ?", task.Id).Update("clients", next).Error; err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return ReloadPingSchedule()
	}
	return nil
}

func GetPingRecords(uuid string, taskId int, start, end time.Time) ([]models.PingRecord, error) {
	return metricstore.GetPingRecords(context.Background(), uuid, taskId, start, end)
}
