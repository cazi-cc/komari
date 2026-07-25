package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/komari-monitor/komari/database/auditlog"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"

	"github.com/komari-monitor/komari/internal/config"
	"github.com/komari-monitor/komari/pkg/rpc"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils"
	"github.com/komari-monitor/komari/utils/geoip"
	"github.com/komari-monitor/komari/utils/messageSender"
	"github.com/komari-monitor/komari/utils/visitorsecurity"
	agent_runtime "github.com/komari-monitor/komari/web/agent"
	"gorm.io/gorm"
)

// admin.system.go
// 系统/运维类 RPC2 方法（admin 命名空间）：日志、远程执行、测试。

func init() {
	RegisterWithGroupAndMeta("getLogs", rpc.RoleAdmin, adminGetLogs, &rpc.MethodMeta{
		Name:    "admin:getLogs",
		Summary: "Get audit logs (paged, optionally filtered by message type)",
		Params: []rpc.ParamMeta{
			{Name: "limit", Type: "string", Description: "Page size (default 100)"},
			{Name: "page", Type: "string", Description: "One-based page number (default 1)"},
			{Name: "msg_type", Type: "string", Description: "Optional exact message type filter"},
		},
		Returns: "{ logs: Log[], total: number }",
	})
	RegisterWithGroupAndMeta("getVisitorLogs", rpc.RoleAdmin, adminGetVisitorLogs, &rpc.MethodMeta{
		Name:    "admin:getVisitorLogs",
		Summary: "Get structured recent visitor page-view records",
		Params: []rpc.ParamMeta{
			{Name: "limit", Type: "number", Description: "Page size (default 50, maximum 200)"},
			{Name: "page", Type: "number", Description: "One-based page number (default 1)"},
		},
		Returns: "{ visitors: VisitorLog[], total: number }",
	})
	RegisterWithGroupAndMeta("getVisitorSecuritySettings", rpc.RoleAdmin, adminGetVisitorSecuritySettings, &rpc.MethodMeta{
		Name:    "admin:getVisitorSecuritySettings",
		Summary: "Get visitor notification, whitelist, and blocklist settings",
		Returns: "VisitorSecuritySettings",
	})
	RegisterWithGroupAndMeta("updateVisitorSecuritySettings", rpc.RoleAdmin, adminUpdateVisitorSecuritySettings, &rpc.MethodMeta{
		Name:    "admin:updateVisitorSecuritySettings",
		Summary: "Update visitor notification, whitelist, and blocklist settings",
		Returns: "VisitorSecuritySettings",
	})
	reg("exec", adminExec, "Execute a command on clients")

	reg("testSendMessage", adminTestSendMessage, "Send a test notification")
	reg("testGeoip", adminTestGeoip, "Test GeoIP lookup")
	// 远程命令执行属敏感操作：除 admin 角色外，还需通过敏感操作二次验证。
	rpc.MarkSensitive("admin:exec")
}

func adminGetLogs(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		Limit   string `json:"limit"`
		Page    string `json:"page"`
		MsgType string `json:"msg_type"`
	}
	req.BindParams(&params)
	if params.Limit == "" {
		params.Limit = "100"
	}
	if params.Page == "" {
		params.Page = "1"
	}
	limitInt, err := strconv.Atoi(params.Limit)
	if err != nil || limitInt <= 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "Invalid limit: "+params.Limit, nil)
	}
	pageInt, err := strconv.Atoi(params.Page)
	if err != nil || pageInt <= 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "Invalid page: "+params.Page, nil)
	}
	db := dbcore.GetDBInstance()
	logs, total, err := queryAdminLogs(db, limitInt, pageInt, params.MsgType)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to retrieve logs: "+err.Error(), nil)
	}
	return map[string]any{"logs": logs, "total": total}, nil
}

func queryAdminLogs(db *gorm.DB, limit, page int, msgType string) ([]models.Log, int64, error) {
	var logs []models.Log
	var total int64
	offset := (page - 1) * limit
	countQuery := filterAdminLogsByMessageType(db.Model(&models.Log{}), msgType)
	logsQuery := filterAdminLogsByMessageType(db.Model(&models.Log{}), msgType)
	if err := countQuery.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	if err := logsQuery.Order("time desc").Limit(limit).Offset(offset).Find(&logs).Error; err != nil {
		return nil, 0, err
	}
	return logs, total, nil
}

func filterAdminLogsByMessageType(query *gorm.DB, msgType string) *gorm.DB {
	if msgType = strings.TrimSpace(msgType); msgType != "" {
		return query.Where("msg_type = ?", msgType)
	}
	return query
}

type adminVisitorLog struct {
	ID        uint           `json:"id"`
	IP        string         `json:"ip"`
	Time      time.Time      `json:"time"`
	Event     string         `json:"event"`
	Path      string         `json:"path,omitempty"`
	Route     string         `json:"route,omitempty"`
	Target    string         `json:"target,omitempty"`
	UserAgent string         `json:"user_agent,omitempty"`
	Detail    map[string]any `json:"detail,omitempty"`
	Geo       *geoip.GeoInfo `json:"geo,omitempty"`
	GeoError  string         `json:"geo_error,omitempty"`
}

func adminGetVisitorLogs(_ context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		Limit int `json:"limit"`
		Page  int `json:"page"`
	}
	req.BindParams(&params)
	if params.Limit == 0 {
		params.Limit = 50
	}
	if params.Page == 0 {
		params.Page = 1
	}
	if params.Limit < 1 || params.Limit > 200 || params.Page < 1 {
		return nil, rpc.MakeError(rpc.InvalidParams, "limit must be 1-200 and page must be positive", nil)
	}

	logs, total, err := queryAdminLogs(dbcore.GetDBInstance(), params.Limit, params.Page, "visitor")
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to retrieve visitor logs: "+err.Error(), nil)
	}

	visitors := make([]adminVisitorLog, 0, len(logs))
	for _, entry := range logs {
		if visitor, ok := parseAdminVisitorLog(entry); ok {
			visitors = append(visitors, visitor)
		}
	}
	enrichAdminVisitorGeo(visitors)
	return map[string]any{"visitors": visitors, "total": total}, nil
}

func enrichAdminVisitorGeo(visitors []adminVisitorLog) {
	indexes := make(map[string][]int)
	for index := range visitors {
		ip := strings.TrimSpace(visitors[index].IP)
		if ip != "" {
			indexes[ip] = append(indexes[ip], index)
		}
	}

	semaphore := make(chan struct{}, 4)
	var wait sync.WaitGroup
	for ip, visitorIndexes := range indexes {
		ip, visitorIndexes := ip, visitorIndexes
		wait.Add(1)
		go func() {
			defer wait.Done()
			semaphore <- struct{}{}
			record, lookupErr := lookupAdminVisitorGeo(ip)
			<-semaphore
			for _, index := range visitorIndexes {
				visitors[index].Geo = record
				if lookupErr != nil {
					visitors[index].GeoError = lookupErr.Error()
				}
			}
		}()
	}
	wait.Wait()
}

func lookupAdminVisitorGeo(ip string) (*geoip.GeoInfo, error) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return nil, fmt.Errorf("无效 IP 地址")
	}
	switch {
	case parsed.IsLoopback():
		return &geoip.GeoInfo{Name: "本机回环地址（历史代理记录）", Provider: "local"}, nil
	case parsed.IsPrivate():
		return &geoip.GeoInfo{Name: "私有网络地址", Provider: "local"}, nil
	case !parsed.IsGlobalUnicast():
		return &geoip.GeoInfo{Name: "非公网地址", Provider: "local"}, nil
	default:
		return geoip.GetGeoInfo(parsed)
	}
}

func adminGetVisitorSecuritySettings(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	settings, err := visitorsecurity.Load()
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to load visitor security settings: "+err.Error(), nil)
	}
	return settings, nil
}

func adminUpdateVisitorSecuritySettings(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var settings visitorsecurity.Settings
	if err := req.BindParams(&settings); err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, "Invalid visitor security settings: "+err.Error(), nil)
	}
	normalized, _, _, err := visitorsecurity.NormalizeAndParse(settings)
	if err != nil {
		return nil, rpc.MakeError(rpc.InvalidParams, err.Error(), nil)
	}
	if meta := rpc.MetaFromContext(ctx); meta != nil {
		blocked, matchErr := visitorsecurity.MatchesBlocklist(normalized.IPBlocklist, meta.RemoteIP)
		if matchErr != nil {
			return nil, rpc.MakeError(rpc.InvalidParams, matchErr.Error(), nil)
		}
		if blocked {
			return nil, rpc.MakeError(rpc.InvalidParams, "当前管理员 IP 不能加入封禁名单", nil)
		}
	}

	if err := config.SetMany(map[string]any{
		config.VisitorNotificationEnabledKey:         normalized.NotificationEnabled,
		config.VisitorNotificationCooldownMinutesKey: normalized.NotificationCooldownMinutes,
		config.VisitorNotificationWhitelistKey:       normalized.NotificationWhitelist,
		config.VisitorIPBlocklistKey:                 normalized.IPBlocklist,
	}); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to save visitor security settings: "+err.Error(), nil)
	}
	if err := visitorsecurity.Update(normalized); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to apply visitor security settings: "+err.Error(), nil)
	}
	actor, ip := auditActor(ctx)
	auditlog.Log(ip, actor, "visitor security settings updated", "info")
	return normalized, nil
}

func parseAdminVisitorLog(entry models.Log) (adminVisitorLog, bool) {
	encoded, ok := strings.CutPrefix(entry.Message, visitorAuditMessagePrefix)
	if !ok {
		return adminVisitorLog{}, false
	}
	var message visitorAuditMessage
	if err := json.Unmarshal([]byte(encoded), &message); err != nil {
		return adminVisitorLog{}, false
	}
	return adminVisitorLog{
		ID:        entry.ID,
		IP:        entry.IP,
		Time:      entry.Time,
		Event:     message.Event,
		Path:      message.Path,
		Route:     message.Route,
		Target:    message.Target,
		UserAgent: message.UserAgent,
		Detail:    message.Detail,
	}, true
}

func adminExec(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		Command string   `json:"command"`
		Clients []string `json:"clients"`
	}

	req.BindParams(&params)
	if strings.TrimSpace(params.Command) == "" {
		return nil, rpc.MakeError(rpc.InvalidParams, "Command cannot be empty", nil)
	}
	if len(params.Clients) == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "clients is required", nil)
	}

	var onlineClients, queuedClients, offlineClients []string
	for _, uuid := range params.Clients {
		if client := agent_runtime.GetConnectedClients()[uuid]; client != nil {
			onlineClients = append(onlineClients, uuid)
		} else if agent_runtime.IsAgentOnline(uuid) {
			queuedClients = append(queuedClients, uuid)
		} else {
			offlineClients = append(offlineClients, uuid)
		}
	}
	if len(onlineClients) == 0 && len(queuedClients) == 0 {
		return nil, rpc.MakeError(rpc.InvalidParams, "No clients connected", nil)
	}
	taskId := utils.GenerateRandomString(16)
	taskClients := append(append([]string{}, onlineClients...), queuedClients...)
	taskClients = append(taskClients, offlineClients...)
	if err := tasks.CreateTask(taskId, taskClients, params.Command); err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to create task: "+err.Error(), nil)
	}
	for _, uuid := range onlineClients {
		legacy := struct {
			Message string `json:"message"`
			Command string `json:"command"`
			TaskId  string `json:"task_id"`
		}{Message: "exec", Command: params.Command, TaskId: taskId}
		payload, _ := json.Marshal(legacy)
		if agent_runtime.IsV2Client(uuid) {
			payload, _ = json.Marshal(v2.Request{JSONRPC: v2.Version, Method: v2.MethodAgentExec, Params: v2.ExecParams{TaskID: taskId, Command: params.Command}})
		}
		client := agent_runtime.GetConnectedClients()[uuid]
		if client == nil {
			return nil, rpc.MakeError(rpc.InvalidParams, "Client connection is null: "+uuid, nil)
		}
		if err := client.WriteMessage(websocket.TextMessage, payload); err != nil {
			return nil, rpc.MakeError(rpc.InvalidParams, "Client connection is broke: "+uuid, nil)
		}
	}
	for _, uuid := range queuedClients {
		agent_runtime.DispatchV2Event(uuid, v2.MethodAgentExec, v2.ExecParams{TaskID: taskId, Command: params.Command})
	}
	actor, ip := auditActor(ctx)
	auditlog.Log(ip, actor, "REC, task id: "+taskId, "warn")
	if len(offlineClients) > 0 {
		for _, uuid := range offlineClients {
			tasks.SaveTaskResult(taskId, uuid, "Client offline!", -1, time.Now().UTC())
		}
	}
	return map[string]any{
		"task_id":        taskId,
		"clients":        onlineClients,
		"queued_clients": queuedClients,
	}, nil
}

func adminTestSendMessage(_ context.Context, _ *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	err := messageSender.SendEvent(models.EventMessage{
		Event:   "Test",
		Time:    time.Now().UTC(),
		Message: "This is a test message from Komari.",
	})
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to send message: "+err.Error(), nil)
	}
	return nil, nil
}

func adminTestGeoip(ctx context.Context, req *rpc.JsonRpcRequest) (any, *rpc.JsonRpcError) {
	var params struct {
		IP string `json:"ip"`
	}
	req.BindParams(&params)
	ip := params.IP
	if ip == "" {
		if meta := rpc.MetaFromContext(ctx); meta != nil {
			ip = meta.RemoteIP
		}
	}
	cfg, err := config.GetAs[bool](config.GeoIpEnabledKey, false)
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to get configuration: "+err.Error(), nil)
	}
	if !cfg {
		return nil, rpc.MakeError(rpc.InvalidParams, "GeoIP is not enabled in the configuration.", nil)
	}
	record, err := geoip.GetGeoInfo(net.ParseIP(ip))
	if err != nil {
		return nil, rpc.MakeError(rpc.InternalError, "Failed to get GeoIP record: "+err.Error(), nil)
	}
	return record, nil
}
