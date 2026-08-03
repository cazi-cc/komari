package tasks

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"gorm.io/gorm"
)

func TestGetAllTCPQualityTasksExcludesDiagnostics(t *testing.T) {
	flags.DatabaseType = flags.DatabaseTypeSQLite
	flags.DatabaseFile = "file:tcp_quality_diagnostic_filter?mode=memory&cache=shared"
	db := dbcore.GetDBInstance()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	normal := models.TCPQualityTask{Name: "normal-" + suffix, Enabled: true, Interval: 900}
	diagnostic := models.TCPQualityTask{Name: "diagnostic-" + suffix, Enabled: true, Interval: 900, Diagnostic: true}
	if err := db.Create(&normal).Error; err != nil {
		t.Fatalf("create normal task: %v", err)
	}
	if err := db.Create(&diagnostic).Error; err != nil {
		t.Fatalf("create diagnostic task: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Delete(&models.TCPQualityTask{}, []uint{normal.Id, diagnostic.Id}).Error
	})

	list, err := GetAllTCPQualityTasks()
	if err != nil {
		t.Fatalf("get task list: %v", err)
	}
	for _, task := range list {
		if task.Id == diagnostic.Id {
			t.Fatal("diagnostic task leaked into scheduled/public task list")
		}
	}
}

func TestDeleteTCPQualityTaskDowngradesManagedPing(t *testing.T) {
	flags.DatabaseType = flags.DatabaseTypeSQLite
	flags.DatabaseFile = "file:tcp_quality_delete_downgrade?mode=memory&cache=shared"
	db := dbcore.GetDBInstance()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	tcpTask := models.TCPQualityTask{Name: "tcp-" + suffix, Enabled: true, Interval: 900}
	if err := db.Create(&tcpTask).Error; err != nil {
		t.Fatalf("create TCP task: %v", err)
	}
	pingTask := models.PingTask{
		Name: "ping-" + suffix, Type: "icmp", Target: "192.0.2.1", Interval: 60,
		ManagedByTCPTask: tcpTask.Id, CatalogTargetKey: "test-ct-v4", SchedulePhaseMS: -1,
	}
	if err := db.Create(&pingTask).Error; err != nil {
		t.Fatalf("create managed ping task: %v", err)
	}
	if err := db.Model(&models.TCPQualityTask{}).Where("id = ?", tcpTask.Id).
		Update("icmp_task_ids", models.StringArray{fmt.Sprint(pingTask.Id)}).Error; err != nil {
		t.Fatalf("bind managed ping task: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Delete(&models.PingTask{}, pingTask.Id).Error
		_ = db.Delete(&models.TCPQualityTask{}, tcpTask.Id).Error
	})

	if err := DeleteTCPQualityTasks([]uint{tcpTask.Id}); err != nil {
		t.Fatalf("delete TCP task: %v", err)
	}
	var preserved models.PingTask
	if err := db.First(&preserved, "id = ?", pingTask.Id).Error; err != nil {
		t.Fatalf("managed ping task was not preserved: %v", err)
	}
	if preserved.ManagedByTCPTask != 0 || preserved.CatalogTargetKey != "" {
		t.Fatalf("managed ping task was not downgraded: %#v", preserved)
	}
	if err := db.First(&models.TCPQualityTask{}, "id = ?", tcpTask.Id).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("TCP task lookup error = %v, want record not found", err)
	}
}

func TestClearExpiredTCPQualityDiagnosticsKeepsActiveEntries(t *testing.T) {
	flags.DatabaseType = flags.DatabaseTypeSQLite
	flags.DatabaseFile = "file:tcp_quality_diagnostic_cleanup?mode=memory&cache=shared"
	db := dbcore.GetDBInstance()
	now := time.Now().UTC()
	expiredAt := now.Add(-time.Minute)
	activeAt := now.Add(time.Hour)
	expired := models.TCPQualityTask{Name: "expired-diagnostic", Diagnostic: true, ExpiresAt: &expiredAt}
	active := models.TCPQualityTask{Name: "active-diagnostic", Diagnostic: true, ExpiresAt: &activeAt}
	if err := db.Create(&expired).Error; err != nil {
		t.Fatalf("create expired diagnostic: %v", err)
	}
	if err := db.Create(&active).Error; err != nil {
		t.Fatalf("create active diagnostic: %v", err)
	}
	if err := db.Create(&models.TCPQualityRun{TaskID: expired.Id, Client: "node", RunID: "expired-run", Payload: "x", FinishedAt: now}).Error; err != nil {
		t.Fatalf("create diagnostic run: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Where("task_id IN ?", []uint{expired.Id, active.Id}).Delete(&models.TCPQualityRun{}).Error
		_ = db.Delete(&models.TCPQualityTask{}, []uint{expired.Id, active.Id}).Error
	})

	if err := ClearExpiredTCPQualityDiagnostics(now); err != nil {
		t.Fatalf("clear diagnostics: %v", err)
	}
	if err := db.First(&models.TCPQualityTask{}, "id = ?", expired.Id).Error; !errors.Is(err, gorm.ErrRecordNotFound) {
		t.Fatalf("expired diagnostic lookup error = %v, want record not found", err)
	}
	if err := db.First(&models.TCPQualityTask{}, "id = ?", active.Id).Error; err != nil {
		t.Fatalf("active diagnostic was removed: %v", err)
	}
	var runCount int64
	if err := db.Model(&models.TCPQualityRun{}).Where("task_id = ?", expired.Id).Count(&runCount).Error; err != nil {
		t.Fatalf("count expired runs: %v", err)
	}
	if runCount != 0 {
		t.Fatalf("expired diagnostic run count = %d, want 0", runCount)
	}
}
