package visitorsecurity

import (
	"net/netip"
	"testing"
	"time"
)

func TestParseRulesAndMatch(t *testing.T) {
	rules, err := ParseRules("192.0.2.7, 2001:db8::/32\n192.0.2.7")
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected two unique rules, got %d", len(rules))
	}
	if !matchIP(rules, "192.0.2.7") || !matchIP(rules, "2001:db8::99") {
		t.Fatal("expected exact IPv4 and IPv6 CIDR to match")
	}
	if matchIP(rules, "192.0.2.8") {
		t.Fatal("unexpected match outside exact IPv4 rule")
	}
}

func TestShouldNotifyCooldownAndWhitelist(t *testing.T) {
	lastNotificationByIP = make(map[netip.Addr]time.Time)
	err := Update(Settings{
		NotificationEnabled:         true,
		NotificationCooldownMinutes: 60,
		NotificationWhitelist:       "203.0.113.0/24",
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	if ShouldNotify("203.0.113.10", now) {
		t.Fatal("whitelisted address should not notify")
	}
	if !ShouldNotify("8.8.8.8", now) {
		t.Fatal("first global address visit should notify")
	}
	if ShouldNotify("8.8.8.8", now.Add(30*time.Minute)) {
		t.Fatal("visit inside cooldown should not notify")
	}
	if !ShouldNotify("8.8.8.8", now.Add(61*time.Minute)) {
		t.Fatal("visit after cooldown should notify")
	}
}
