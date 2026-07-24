package jsonrpc

import (
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func TestRedactGuestNodeRemovesAddressAndAdminFields(t *testing.T) {
	input := models.Client{
		UUID:         "node-1",
		Name:         "public node",
		IPv4:         "203.0.113.10",
		IPv6:         "2001:db8::10",
		Remark:       "private remark",
		PublicRemark: "public remark",
		Version:      "1.0.0",
		Token:        "secret",
	}

	got := redactGuestNode(input)
	if got.IPv4 != "" || got.IPv6 != "" || got.Remark != "" || got.Version != "" || got.Token != "" {
		t.Fatalf("guest node still contains sensitive fields: %#v", got)
	}
	if got.UUID != input.UUID || got.Name != input.Name || got.PublicRemark != input.PublicRemark {
		t.Fatalf("guest-safe public fields changed unexpectedly: %#v", got)
	}
}
