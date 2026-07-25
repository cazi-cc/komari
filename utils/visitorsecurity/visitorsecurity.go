package visitorsecurity

import (
	"fmt"
	"net/netip"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/komari-monitor/komari/internal/config"
)

const (
	DefaultNotificationCooldownMinutes = 1440
	MaxNotificationCooldownMinutes     = 10080
	notificationStateTTL               = 30 * 24 * time.Hour
)

type Settings struct {
	NotificationEnabled         bool   `json:"notification_enabled"`
	NotificationCooldownMinutes int    `json:"notification_cooldown_minutes"`
	NotificationWhitelist       string `json:"notification_whitelist"`
	IPBlocklist                 string `json:"ip_blocklist"`
}

type snapshot struct {
	settings          Settings
	notificationRules []netip.Prefix
	blockRules        []netip.Prefix
}

var (
	current atomic.Pointer[snapshot]

	notificationMu       sync.Mutex
	lastNotificationByIP = make(map[netip.Addr]time.Time)
)

func Load() (Settings, error) {
	values, err := config.GetMany(map[string]any{
		config.VisitorNotificationEnabledKey:         false,
		config.VisitorNotificationCooldownMinutesKey: DefaultNotificationCooldownMinutes,
		config.VisitorNotificationWhitelistKey:       "",
		config.VisitorIPBlocklistKey:                 "",
	})
	if err != nil {
		return Settings{}, err
	}
	return Settings{
		NotificationEnabled:         boolValue(values[config.VisitorNotificationEnabledKey]),
		NotificationCooldownMinutes: intValue(values[config.VisitorNotificationCooldownMinutesKey], DefaultNotificationCooldownMinutes),
		NotificationWhitelist:       stringValue(values[config.VisitorNotificationWhitelistKey]),
		IPBlocklist:                 stringValue(values[config.VisitorIPBlocklistKey]),
	}, nil
}

func Reload() error {
	settings, err := Load()
	if err != nil {
		return err
	}
	return Update(settings)
}

func Update(settings Settings) error {
	normalized, notificationRules, blockRules, err := NormalizeAndParse(settings)
	if err != nil {
		return err
	}
	current.Store(&snapshot{
		settings:          normalized,
		notificationRules: notificationRules,
		blockRules:        blockRules,
	})
	return nil
}

func NormalizeAndParse(settings Settings) (Settings, []netip.Prefix, []netip.Prefix, error) {
	if settings.NotificationCooldownMinutes < 1 || settings.NotificationCooldownMinutes > MaxNotificationCooldownMinutes {
		return Settings{}, nil, nil, fmt.Errorf("notification cooldown must be between 1 and %d minutes", MaxNotificationCooldownMinutes)
	}

	notificationRules, err := ParseRules(settings.NotificationWhitelist)
	if err != nil {
		return Settings{}, nil, nil, fmt.Errorf("invalid notification whitelist: %w", err)
	}
	blockRules, err := ParseRules(settings.IPBlocklist)
	if err != nil {
		return Settings{}, nil, nil, fmt.Errorf("invalid IP blocklist: %w", err)
	}
	settings.NotificationWhitelist = FormatRules(notificationRules)
	settings.IPBlocklist = FormatRules(blockRules)
	return settings, notificationRules, blockRules, nil
}

func ParseRules(input string) ([]netip.Prefix, error) {
	fields := strings.FieldsFunc(input, func(r rune) bool {
		return r == ',' || r == ';' || r == '\n' || r == '\r' || r == '\t' || r == ' '
	})
	seen := make(map[netip.Prefix]struct{}, len(fields))
	rules := make([]netip.Prefix, 0, len(fields))
	for _, field := range fields {
		value := strings.TrimSpace(field)
		if value == "" || strings.HasPrefix(value, "#") {
			continue
		}
		prefix, err := parseRule(value)
		if err != nil {
			return nil, fmt.Errorf("%q is not a valid IP address or CIDR", value)
		}
		if _, ok := seen[prefix]; ok {
			continue
		}
		seen[prefix] = struct{}{}
		rules = append(rules, prefix)
	}
	sort.Slice(rules, func(i, j int) bool {
		return rules[i].String() < rules[j].String()
	})
	return rules, nil
}

func parseRule(value string) (netip.Prefix, error) {
	if prefix, err := netip.ParsePrefix(value); err == nil {
		return prefix.Masked(), nil
	}
	address, err := netip.ParseAddr(value)
	if err != nil {
		return netip.Prefix{}, err
	}
	address = address.Unmap()
	return netip.PrefixFrom(address, address.BitLen()), nil
}

func FormatRules(rules []netip.Prefix) string {
	values := make([]string, 0, len(rules))
	for _, rule := range rules {
		values = append(values, rule.String())
	}
	return strings.Join(values, "\n")
}

func IsBlocked(ip string) bool {
	state := current.Load()
	return state != nil && matchIP(state.blockRules, ip)
}

func IsNotificationExempt(ip string) bool {
	state := current.Load()
	return state != nil && matchIP(state.notificationRules, ip)
}

func MatchesBlocklist(blocklist, ip string) (bool, error) {
	rules, err := ParseRules(blocklist)
	if err != nil {
		return false, err
	}
	return matchIP(rules, ip), nil
}

func ShouldNotify(ip string, now time.Time) bool {
	state := current.Load()
	if state == nil || !state.settings.NotificationEnabled || matchIP(state.notificationRules, ip) {
		return false
	}
	address, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return false
	}
	address = address.Unmap()
	if !address.IsGlobalUnicast() || address.IsPrivate() || address.IsLoopback() {
		return false
	}

	cooldown := time.Duration(state.settings.NotificationCooldownMinutes) * time.Minute
	notificationMu.Lock()
	defer notificationMu.Unlock()
	for candidate, sentAt := range lastNotificationByIP {
		if now.Sub(sentAt) > notificationStateTTL {
			delete(lastNotificationByIP, candidate)
		}
	}
	if sentAt, ok := lastNotificationByIP[address]; ok && now.Sub(sentAt) < cooldown {
		return false
	}
	lastNotificationByIP[address] = now
	return true
}

func matchIP(rules []netip.Prefix, ip string) bool {
	address, err := netip.ParseAddr(strings.TrimSpace(ip))
	if err != nil {
		return false
	}
	address = address.Unmap()
	for _, rule := range rules {
		if rule.Contains(address) {
			return true
		}
	}
	return false
}

func boolValue(value any) bool {
	result, _ := value.(bool)
	return result
}

func stringValue(value any) string {
	result, _ := value.(string)
	return result
}

func intValue(value any, fallback int) int {
	switch typed := value.(type) {
	case int:
		return typed
	case float64:
		return int(typed)
	default:
		return fallback
	}
}
