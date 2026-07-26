package utils

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	v2 "github.com/komari-monitor/komari/protocol/v2"
)

const (
	tcpQualityCatalogURL      = "https://tcpquality.ibsgss.uk/getNodes"
	tcpQualityCatalogMaxAge   = 10 * time.Minute
	tcpQualityCatalogMaxBytes = 4 << 20
)

type tcpQualityProviderCatalog struct {
	Version     int                        `json:"version"`
	GeneratedAt time.Time                  `json:"generatedAt"`
	CDN         []tcpQualityProviderTarget `json:"cdn"`
}

type tcpQualityProviderTarget struct {
	Province     string `json:"province"`
	ProvinceCode string `json:"provinceCode"`
	ISP          string `json:"isp"`
	ISPCode      string `json:"ispCode"`
	IPVersion    int    `json:"ipVersion"`
	Port         int    `json:"port"`
	IP           string `json:"ip"`
}

type TCPQualityCatalogOption struct {
	Code string `json:"code"`
	Name string `json:"name"`
}

type TCPQualityCatalogView struct {
	Revision     string                    `json:"revision"`
	GeneratedAt  time.Time                 `json:"generated_at"`
	LastSyncedAt time.Time                 `json:"last_synced_at"`
	Provinces    []TCPQualityCatalogOption `json:"provinces"`
	ISPs         []TCPQualityCatalogOption `json:"isps"`
	IPVersions   []int                     `json:"ip_versions"`
	TargetCount  int                       `json:"target_count"`
}

type TCPQualityTargetLabel struct {
	Key          string `json:"key"`
	Province     string `json:"province"`
	ProvinceCode string `json:"province_code"`
	ISP          string `json:"isp"`
	ISPCode      string `json:"isp_code"`
	IPVersion    int    `json:"ip_version"`
}

type tcpQualityCatalogState struct {
	View    TCPQualityCatalogView
	Targets []v2.TCPQualityTarget
}

func GetTCPQualityCatalog(ctx context.Context, force bool) (TCPQualityCatalogView, error) {
	state, err := loadTCPQualityCatalog(ctx, force)
	return state.View, err
}

func GetTCPQualityTargetLabels(ctx context.Context, task models.TCPQualityTask) ([]TCPQualityTargetLabel, string, error) {
	catalog, err := loadTCPQualityCatalog(ctx, false)
	if err != nil {
		return nil, "", err
	}
	selected := selectTCPQualityTargets(task, catalog)
	labels := make([]TCPQualityTargetLabel, 0, len(selected))
	for _, target := range selected {
		labels = append(labels, TCPQualityTargetLabel{
			Key:          target.Key,
			Province:     target.Province,
			ProvinceCode: target.ProvinceCode,
			ISP:          target.ISP,
			ISPCode:      target.ISPCode,
			IPVersion:    target.IPVersion,
		})
	}
	return labels, catalog.View.Revision, nil
}

func loadTCPQualityCatalog(ctx context.Context, force bool) (tcpQualityCatalogState, error) {
	cached, cacheErr := readTCPQualityCatalogCache()
	if !force && cacheErr == nil && time.Since(cached.LastSyncedAt) < tcpQualityCatalogMaxAge {
		return decodeTCPQualityCatalogCache(cached)
	}

	fresh, err := fetchTCPQualityCatalog(ctx)
	if err == nil {
		if saveErr := saveTCPQualityCatalogCache(fresh); saveErr != nil {
			return tcpQualityCatalogState{}, saveErr
		}
		return decodeTCPQualityCatalogCache(fresh)
	}
	if cacheErr == nil {
		return decodeTCPQualityCatalogCache(cached)
	}
	return tcpQualityCatalogState{}, fmt.Errorf("refresh TCP quality catalog: %w", err)
}

func readTCPQualityCatalogCache() (models.TCPQualityCatalogCache, error) {
	var cached models.TCPQualityCatalogCache
	err := dbcore.GetDBInstance().First(&cached, "id = ?", 1).Error
	return cached, err
}

func fetchTCPQualityCatalog(ctx context.Context) (models.TCPQualityCatalogCache, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, tcpQualityCatalogURL, nil)
	if err != nil {
		return models.TCPQualityCatalogCache{}, err
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "Komari-Cazi-TCPQuality/1.0")

	client := &http.Client{Timeout: 15 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return models.TCPQualityCatalogCache{}, err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return models.TCPQualityCatalogCache{}, fmt.Errorf("catalog returned HTTP %d", response.StatusCode)
	}
	payload, err := io.ReadAll(io.LimitReader(response.Body, tcpQualityCatalogMaxBytes+1))
	if err != nil {
		return models.TCPQualityCatalogCache{}, err
	}
	if len(payload) > tcpQualityCatalogMaxBytes {
		return models.TCPQualityCatalogCache{}, fmt.Errorf("catalog exceeds %d bytes", tcpQualityCatalogMaxBytes)
	}
	if _, err := parseTCPQualityCatalog(payload); err != nil {
		return models.TCPQualityCatalogCache{}, err
	}
	hash := sha256.Sum256(payload)
	return models.TCPQualityCatalogCache{
		Id:           1,
		Revision:     hex.EncodeToString(hash[:8]),
		Payload:      string(payload),
		LastSyncedAt: time.Now().UTC(),
	}, nil
}

func saveTCPQualityCatalogCache(cached models.TCPQualityCatalogCache) error {
	var provider tcpQualityProviderCatalog
	if err := json.Unmarshal([]byte(cached.Payload), &provider); err != nil {
		return err
	}
	cached.GeneratedAt = provider.GeneratedAt.UTC()
	return dbcore.GetDBInstance().Save(&cached).Error
}

func decodeTCPQualityCatalogCache(cached models.TCPQualityCatalogCache) (tcpQualityCatalogState, error) {
	targets, err := parseTCPQualityCatalog([]byte(cached.Payload))
	if err != nil {
		return tcpQualityCatalogState{}, err
	}
	provinces := make(map[string]string)
	isps := make(map[string]string)
	versions := make(map[int]struct{})
	for _, target := range targets {
		provinces[target.ProvinceCode] = target.Province
		isps[target.ISPCode] = target.ISP
		versions[target.IPVersion] = struct{}{}
	}
	view := TCPQualityCatalogView{
		Revision:     cached.Revision,
		GeneratedAt:  cached.GeneratedAt.UTC(),
		LastSyncedAt: cached.LastSyncedAt.UTC(),
		TargetCount:  len(targets),
	}
	for code, name := range provinces {
		view.Provinces = append(view.Provinces, TCPQualityCatalogOption{Code: code, Name: name})
	}
	for code, name := range isps {
		view.ISPs = append(view.ISPs, TCPQualityCatalogOption{Code: code, Name: name})
	}
	for version := range versions {
		view.IPVersions = append(view.IPVersions, version)
	}
	sort.Slice(view.Provinces, func(i, j int) bool { return view.Provinces[i].Code < view.Provinces[j].Code })
	sort.Slice(view.ISPs, func(i, j int) bool { return view.ISPs[i].Code < view.ISPs[j].Code })
	sort.Ints(view.IPVersions)
	return tcpQualityCatalogState{View: view, Targets: targets}, nil
}

func parseTCPQualityCatalog(payload []byte) ([]v2.TCPQualityTarget, error) {
	var provider tcpQualityProviderCatalog
	if err := json.Unmarshal(payload, &provider); err != nil {
		return nil, fmt.Errorf("decode catalog: %w", err)
	}
	if provider.Version <= 0 || len(provider.CDN) == 0 || len(provider.CDN) > 512 {
		return nil, fmt.Errorf("catalog has invalid version or target count")
	}
	seen := make(map[string]struct{}, len(provider.CDN))
	targets := make([]v2.TCPQualityTarget, 0, len(provider.CDN))
	for _, item := range provider.CDN {
		item.ProvinceCode = normalizeTCPQualityCode(item.ProvinceCode)
		item.ISPCode = normalizeTCPQualityCode(item.ISPCode)
		address := strings.TrimSpace(item.IP)
		ip := net.ParseIP(address)
		if item.ProvinceCode == "" || item.ISPCode == "" || ip == nil || item.Port < 1 || item.Port > 65535 {
			continue
		}
		version := 6
		if ip.To4() != nil {
			version = 4
		}
		if item.IPVersion != version {
			continue
		}
		key := item.ProvinceCode + "-" + item.ISPCode + "-v" + strconv.Itoa(version)
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}
		targets = append(targets, v2.TCPQualityTarget{
			Key:          key,
			Address:      address,
			Port:         item.Port,
			Province:     strings.TrimSpace(item.Province),
			ProvinceCode: item.ProvinceCode,
			ISP:          strings.TrimSpace(item.ISP),
			ISPCode:      item.ISPCode,
			IPVersion:    version,
		})
	}
	if len(targets) == 0 {
		return nil, fmt.Errorf("catalog contains no valid targets")
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].Key < targets[j].Key })
	return targets, nil
}

func normalizeTCPQualityCode(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" || len(value) > 16 {
		return ""
	}
	for _, char := range value {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' {
			return ""
		}
	}
	return value
}

func NormalizeTCPQualityTask(task *models.TCPQualityTask) (int, int, error) {
	if task == nil {
		return 0, 0, fmt.Errorf("task is required")
	}
	task.Name = strings.TrimSpace(task.Name)
	if task.Name == "" || len(task.Name) > 255 {
		return 0, 0, fmt.Errorf("name is required and cannot exceed 255 characters")
	}
	task.Clients = uniqueTCPQualityValues(task.Clients, false)
	task.ProvinceCodes = uniqueTCPQualityValues(task.ProvinceCodes, true)
	task.ISPCode = uniqueTCPQualityValues(task.ISPCode, true)
	task.IPVersions = uniqueTCPQualityVersions(task.IPVersions)
	task.ICMPTaskIDs = uniqueTCPQualityTaskIDs(task.ICMPTaskIDs)
	if !task.DefaultOn && len(task.Clients) == 0 {
		return 0, 0, fmt.Errorf("clients is required when default_on is false")
	}
	if len(task.ProvinceCodes) == 0 || len(task.ISPCode) == 0 || len(task.IPVersions) == 0 {
		return 0, 0, fmt.Errorf("province_codes, isp_codes and ip_versions are required")
	}
	if task.StandardPackets == 0 {
		task.StandardPackets = 30
	}
	if task.StandardPackets < 10 || task.StandardPackets > 200 {
		return 0, 0, fmt.Errorf("standard_packets must be between 10 and 200")
	}
	if task.LargePackets == 0 {
		task.LargePackets = 30
	}
	if task.LargePackets < 10 || task.LargePackets > 100 {
		return 0, 0, fmt.Errorf("large_packets must be between 10 and 100")
	}
	if task.DelayMS == 0 {
		task.DelayMS = 200
	}
	if task.DelayMS < 50 || task.DelayMS > 5000 {
		return 0, 0, fmt.Errorf("delay_ms must be between 50 and 5000")
	}
	if task.TimeoutMS == 0 {
		task.TimeoutMS = 3000
	}
	if task.TimeoutMS < 500 || task.TimeoutMS > 15000 {
		return 0, 0, fmt.Errorf("timeout_ms must be between 500 and 15000")
	}
	targetCount := len(task.ProvinceCodes) * len(task.ISPCode) * len(task.IPVersions)
	packetCount := targetCount * task.StandardPackets
	if task.LargeEnabled {
		packetCount += targetCount * task.LargePackets
	}
	minimumInterval := MinimumTCPQualityInterval(packetCount)
	if task.Interval < minimumInterval {
		return targetCount, packetCount, fmt.Errorf("interval must be at least %d seconds for %d packets per node", minimumInterval, packetCount)
	}
	return targetCount, packetCount, nil
}

func MinimumTCPQualityInterval(packetCount int) int {
	switch {
	case packetCount <= 1000:
		return 15 * 60
	case packetCount <= 10000:
		return 60 * 60
	default:
		return 6 * 60 * 60
	}
}

func uniqueTCPQualityValues(values models.StringArray, normalize bool) models.StringArray {
	seen := make(map[string]struct{}, len(values))
	result := make(models.StringArray, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if normalize {
			value = normalizeTCPQualityCode(value)
		}
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

func uniqueTCPQualityVersions(values models.StringArray) models.StringArray {
	seen := make(map[string]struct{}, len(values))
	result := make(models.StringArray, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "4" && value != "6" {
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

func uniqueTCPQualityTaskIDs(values models.StringArray) models.StringArray {
	seen := make(map[string]struct{}, len(values))
	result := make(models.StringArray, 0, len(values))
	for _, value := range values {
		id, err := strconv.ParseUint(strings.TrimSpace(value), 10, 32)
		if err != nil || id == 0 {
			continue
		}
		normalized := strconv.FormatUint(id, 10)
		if _, exists := seen[normalized]; exists {
			continue
		}
		seen[normalized] = struct{}{}
		result = append(result, normalized)
	}
	sort.Slice(result, func(i, j int) bool {
		left, _ := strconv.ParseUint(result[i], 10, 32)
		right, _ := strconv.ParseUint(result[j], 10, 32)
		return left < right
	})
	return result
}

func selectTCPQualityTargets(task models.TCPQualityTask, catalog tcpQualityCatalogState) []v2.TCPQualityTarget {
	provinces := stringSet(task.ProvinceCodes)
	isps := stringSet(task.ISPCode)
	versions := stringSet(task.IPVersions)
	selected := make([]v2.TCPQualityTarget, 0)
	for _, target := range catalog.Targets {
		if _, ok := provinces[target.ProvinceCode]; !ok {
			continue
		}
		if _, ok := isps[target.ISPCode]; !ok {
			continue
		}
		if _, ok := versions[strconv.Itoa(target.IPVersion)]; !ok {
			continue
		}
		selected = append(selected, target)
	}
	return selected
}

func stringSet(values []string) map[string]struct{} {
	result := make(map[string]struct{}, len(values))
	for _, value := range values {
		result[value] = struct{}{}
	}
	return result
}
