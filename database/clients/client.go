package clients

import (
	"encoding/json"
	"fmt"
	"math"
	"net"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/database/tasks"
	"github.com/komari-monitor/komari/utils"
	logger "github.com/komari-monitor/komari/utils/log"

	"github.com/google/uuid"
)

func DeleteClient(clientUuid string) error {
	db := dbcore.GetDBInstance()
	err := db.Delete(&models.Client{}, "uuid = ?", clientUuid).Error
	if err != nil {
		return err
	}
	return nil
}

func SaveClientInfo(update map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	clientUUID, ok := update["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	// 确保更新的字段不为空
	if len(update) == 0 {
		return fmt.Errorf("no fields to update")
	}

	update["updated_at"] = time.Now().UTC()

	toFloat64 := func(value interface{}) (float64, bool) {
		switch typed := value.(type) {
		case float64:
			return typed, true
		case float32:
			return float64(typed), true
		case int:
			return float64(typed), true
		case int8:
			return float64(typed), true
		case int16:
			return float64(typed), true
		case int32:
			return float64(typed), true
		case int64:
			return float64(typed), true
		case uint:
			return float64(typed), true
		case uint8:
			return float64(typed), true
		case uint16:
			return float64(typed), true
		case uint32:
			return float64(typed), true
		case uint64:
			return float64(typed), true
		case json.Number:
			parsed, err := typed.Float64()
			if err != nil {
				return 0, false
			}
			return parsed, true
		default:
			return 0, false
		}
	}

	checkOptionalInt := func(name, key string, maxValue float64) error {
		value, exists := update[key]
		if !exists || value == nil {
			return nil
		}

		numericValue, ok := toFloat64(value)
		if !ok {
			return fmt.Errorf("%s must be a valid number", name)
		}
		if numericValue < 0 || numericValue > maxValue {
			return fmt.Errorf("%s must be a valid non-negative number: %v", name, value)
		}
		return nil
	}

	verify := func(update map[string]interface{}) error {
		if err := checkOptionalInt("Cpu.Cores", "cpu_cores", math.MaxInt-1); err != nil {
			return err
		}
		if err := checkOptionalInt("Cpu.PhysicalCores", "cpu_physical_cores", math.MaxInt-1); err != nil {
			return err
		}
		if err := checkOptionalInt("Ram.Total", "mem_total", math.MaxInt64-1); err != nil {
			return err
		}
		if err := checkOptionalInt("Swap.Total", "swap_total", math.MaxInt64-1); err != nil {
			return err
		}
		if err := checkOptionalInt("Disk.Total", "disk_total", math.MaxInt64-1); err != nil {
			return err
		}
		return nil
	}

	if err := verify(update); err != nil {
		return err
	}

	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Updates(update).Error
	if err != nil {
		return err
	}
	return nil
}

// CreateClient 创建新客户端
func CreateClient() (clientUUID, token string, err error) {
	db := dbcore.GetDBInstance()
	token = utils.GenerateToken()
	clientUUID = uuid.New().String()

	client := models.Client{
		UUID:      clientUUID,
		Token:     token,
		Name:      "client_" + clientUUID[0:8],
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	err = db.Create(&client).Error
	if err != nil {
		return "", "", err
	}
	if err := tasks.AddDefaultOnClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on ping tasks to new client:", err)
	}
	if err := tasks.AddDefaultTCPQualityClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on TCP quality tasks to new client:", err)
	}
	if err := tasks.AddDefaultUnlockQualityClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on unlock quality tasks to new client:", err)
	}
	return clientUUID, token, nil
}

func CreateClientWithName(name string) (clientUUID, token string, err error) {
	if name == "" {
		return CreateClient()
	}
	db := dbcore.GetDBInstance()
	token = utils.GenerateToken()
	clientUUID = uuid.New().String()
	client := models.Client{
		UUID:      clientUUID,
		Token:     token,
		Name:      name,
		CreatedAt: time.Now().UTC(),
		UpdatedAt: time.Now().UTC(),
	}

	err = db.Create(&client).Error
	if err != nil {
		return "", "", err
	}
	if err := tasks.AddDefaultOnClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on ping tasks to new client:", err)
	}
	if err := tasks.AddDefaultTCPQualityClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on TCP quality tasks to new client:", err)
	}
	if err := tasks.AddDefaultUnlockQualityClientUUID(clientUUID); err != nil {
		logger.ErrorArgs("clients", "Failed to apply default-on unlock quality tasks to new client:", err)
	}
	return clientUUID, token, nil
}

/*
// GetAllClients 获取所有客户端配置

	func getAllClients() (clients []models.Client, err error) {
		db := dbcore.GetDBInstance()
		err = db.Find(&clients).Error
		if err != nil {
			return nil, err
		}
		return clients, nil
	}
*/
func GetClientByUUID(uuid string) (client models.Client, err error) {
	db := dbcore.GetDBInstance()
	err = db.Where("uuid = ?", uuid).First(&client).Error
	if err != nil {
		return models.Client{}, err
	}
	return client, nil
}

func GetClientTokenByUUID(uuid string) (token string, err error) {
	db := dbcore.GetDBInstance()
	var client models.Client
	err = db.Where("uuid = ?", uuid).First(&client).Error
	if err != nil {
		return "", err
	}
	return client.Token, nil
}

func GetAllClientBasicInfo() (clients []models.Client, err error) {
	db := dbcore.GetDBInstance()
	err = db.Find(&clients).Error
	if err != nil {
		return nil, err
	}
	return clients, nil
}

func SaveClient(updates map[string]interface{}) error {
	db := dbcore.GetDBInstance()
	clientUUID, ok := updates["uuid"].(string)
	if !ok || clientUUID == "" {
		return fmt.Errorf("invalid client UUID")
	}

	// 确保更新的字段不为空
	if len(updates) == 0 {
		return fmt.Errorf("no fields to update")
	}

	if v, exists := updates["traffic_limit"]; exists {
		if val, ok := v.(float64); ok {
			if val < 0 || val > math.MaxInt64-1 {
				return fmt.Errorf("traffic_limit must be a valid non-negative int64 value, got %v", val)
			}
		}
	}
	if value, exists := updates["expired_at"]; exists {
		switch typed := value.(type) {
		case nil:
			updates["expired_at"] = nil
		case time.Time:
			updates["expired_at"] = typed.UTC()
		case *time.Time:
			if typed == nil {
				updates["expired_at"] = nil
			} else {
				updates["expired_at"] = typed.UTC()
			}
		case string:
			stamp, err := time.Parse(time.RFC3339Nano, typed)
			if err != nil {
				return fmt.Errorf("expired_at must be an RFC3339 timestamp with a timezone: %w", err)
			}
			updates["expired_at"] = stamp.UTC()
		default:
			return fmt.Errorf("expired_at must be an RFC3339 timestamp with a timezone")
		}
	}
	if value, exists := updates["reachable_addresses"]; exists {
		addresses, err := normalizeReachableAddresses(value)
		if err != nil {
			return err
		}
		if err := ensureReachableAddressesUnique(clientUUID, addresses); err != nil {
			return err
		}
		updates["reachable_addresses"] = addresses
	}

	updates["updated_at"] = time.Now().UTC()

	err := db.Model(&models.Client{}).Where("uuid = ?", clientUUID).Updates(updates).Error
	if err != nil {
		return err
	}
	return nil
}

const maxReachableAddressesPerClient = 16

func normalizeReachableAddresses(value interface{}) (models.StringArray, error) {
	var values []string
	switch typed := value.(type) {
	case nil:
		return models.StringArray{}, nil
	case []string:
		values = typed
	case models.StringArray:
		values = []string(typed)
	case []interface{}:
		values = make([]string, 0, len(typed))
		for _, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("reachable_addresses must contain only IP address strings")
			}
			values = append(values, text)
		}
	default:
		return nil, fmt.Errorf("reachable_addresses must be an array of IP address strings")
	}
	if len(values) > maxReachableAddressesPerClient {
		return nil, fmt.Errorf("reachable_addresses supports at most %d addresses", maxReachableAddressesPerClient)
	}

	result := make(models.StringArray, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		value = strings.TrimSpace(strings.Trim(strings.TrimSpace(value), "[]"))
		if value == "" {
			continue
		}
		parsed := net.ParseIP(value)
		if parsed == nil {
			return nil, fmt.Errorf("reachable_addresses contains an invalid IP address: %s", value)
		}
		address := parsed.String()
		if _, exists := seen[address]; exists {
			continue
		}
		seen[address] = struct{}{}
		result = append(result, address)
	}
	return result, nil
}

func ensureReachableAddressesUnique(clientUUID string, addresses models.StringArray) error {
	if len(addresses) == 0 {
		return nil
	}
	wanted := make(map[string]struct{}, len(addresses))
	for _, address := range addresses {
		wanted[address] = struct{}{}
	}

	var clients []models.Client
	if err := dbcore.GetDBInstance().
		Select("uuid", "name", "ipv4", "ipv6", "reachable_addresses").
		Where("uuid <> ?", clientUUID).
		Find(&clients).Error; err != nil {
		return fmt.Errorf("validate reachable_addresses: %w", err)
	}
	for _, client := range clients {
		candidates := append([]string{client.IPv4, client.IPv6}, []string(client.ReachableAddresses)...)
		for _, candidate := range candidates {
			parsed := net.ParseIP(strings.TrimSpace(strings.Trim(candidate, "[]")))
			if parsed == nil {
				continue
			}
			if _, conflict := wanted[parsed.String()]; conflict {
				return fmt.Errorf("reachable address %s is already assigned to client %s", parsed.String(), client.Name)
			}
		}
	}
	return nil
}
