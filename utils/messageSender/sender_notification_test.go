package messageSender

import (
	"testing"

	messageevent "github.com/komari-monitor/komari/database/models/messageEvent"
	"github.com/komari-monitor/komari/internal/config"
)

func TestNotificationEventSettingKey(t *testing.T) {
	tests := map[string]string{
		messageevent.Offline: config.NodeStatusNotificationEnabledKey,
		messageevent.Online:  config.NodeStatusNotificationEnabledKey,
		messageevent.Expire:  config.ExpireNotificationEnabledKey,
		messageevent.Renew:   config.RenewalNotificationEnabledKey,
		messageevent.Login:   config.LoginNotificationKey,
		messageevent.Alert:   config.LoadNotificationEnabledKey,
		messageevent.Traffic: config.TrafficNotificationEnabledKey,
		messageevent.DReport: config.TrafficReportNotificationEnabledKey,
		messageevent.WReport: config.TrafficReportNotificationEnabledKey,
		messageevent.MReport: config.TrafficReportNotificationEnabledKey,
		"Test":               "",
	}
	for event, want := range tests {
		if got := notificationEventSettingKey(event); got != want {
			t.Fatalf("event %q: got %q, want %q", event, got, want)
		}
	}
}
