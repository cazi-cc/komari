package messageSender

import (
	"context"
	"github.com/komari-monitor/komari/internal/config"
)

// SendTextMessage preserves the custom probe-alert entry point while routing
// through upstream's current notification-channel registry.
func SendTextMessage(message string, title string) error {
	enabled, err := config.GetAs[bool](config.NotificationEnabledKey, false)
	if err != nil || !enabled {
		return err
	}
	method, err := config.GetAs[string](config.NotificationMethodKey, "none")
	if err != nil || method == "" || method == "none" {
		return err
	}
	return CallNotificationChannel(context.Background(), method, Notification{
		Title: title, Message: message,
	})
}
