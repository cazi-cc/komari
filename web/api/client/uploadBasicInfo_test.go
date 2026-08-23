package client

import (
	"net"
	"testing"
	"time"

	"github.com/komari-monitor/komari/cmd/flags"
	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/internal/config"
	v2 "github.com/komari-monitor/komari/protocol/v2"
	"github.com/komari-monitor/komari/utils/geoip"
)

type staticGeoIPProvider struct {
	name string
	iso  string
}

func (p staticGeoIPProvider) Name() string {
	return p.name
}

func (p staticGeoIPProvider) GetGeoInfo(ip net.IP) (*geoip.GeoInfo, error) {
	return &geoip.GeoInfo{ISOCode: p.iso, Name: p.iso}, nil
}

func (p staticGeoIPProvider) UpdateDatabase() error {
	return nil
}

func (p staticGeoIPProvider) Close() error {
	return nil
}

func TestV2BasicInfoFillsRegionFromGeoIP(t *testing.T) {
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = "file:v2_basic_info_geoip?mode=memory&cache=shared"

	db := dbcore.GetDBInstance()
	if err := config.Set(config.GeoIpEnabledKey, true); err != nil {
		t.Fatalf("enable geoip: %v", err)
	}

	oldProvider := geoip.CurrentProvider
	geoip.CurrentProvider = staticGeoIPProvider{name: t.Name(), iso: "SG"}
	t.Cleanup(func() {
		geoip.CurrentProvider = oldProvider
	})

	clientUUID := "client-v2-geoip"
	now := time.Now().UTC()
	if err := db.Create(&models.Client{
		UUID:      clientUUID,
		Token:     "token-v2-geoip",
		Name:      "client_v2_geoip",
		CreatedAt: now,
		UpdatedAt: now,
	}).Error; err != nil {
		t.Fatalf("create client: %v", err)
	}

	resp := handleV2RPC(clientUUID, v2.Request{
		JSONRPC: v2.Version,
		Method:  v2.MethodAgentBasicInfo,
		Params: map[string]interface{}{
			"info": map[string]interface{}{
				"ipv4": "8.8.8.8",
			},
		},
		ID: "basic-info",
	}, false)
	if resp.Error != nil {
		t.Fatalf("v2 basic info failed: %+v", resp.Error)
	}

	var got models.Client
	if err := db.First(&got, "uuid = ?", clientUUID).Error; err != nil {
		t.Fatalf("load client: %v", err)
	}
	want := geoip.GetRegionUnicodeEmoji("SG")
	if got.Region != want {
		t.Fatalf("expected GeoIP region to be saved, got %q", got.Region)
	}
}

func TestBasicInfoCannotOverwriteAdminOwnedFields(t *testing.T) {
	flags.DatabaseType = "sqlite"
	flags.DatabaseFile = "file:v2_basic_info_geoip?mode=memory&cache=shared"

	db := dbcore.GetDBInstance()
	clientUUID := "client-admin-fields"
	now := time.Now().UTC()
	if err := db.Create(&models.Client{
		UUID:               clientUUID,
		Token:              "token-admin-fields",
		Name:               "managed name",
		PublicRemark:       "managed remark",
		Hidden:             true,
		ReachableAddresses: models.StringArray{"141.11.86.162"},
		CreatedAt:          now,
		UpdatedAt:          now,
	}).Error; err != nil {
		t.Fatalf("create client: %v", err)
	}

	if err := saveClientBasicInfo(map[string]interface{}{
		"ipv4":                "61.224.64.204",
		"region":              "forged region",
		"name":                "agent supplied name",
		"public_remark":       "agent supplied remark",
		"hidden":              false,
		"reachable_addresses": []interface{}{},
	}, clientUUID, ""); err != nil {
		t.Fatalf("save agent basic info: %v", err)
	}

	var got models.Client
	if err := db.First(&got, "uuid = ?", clientUUID).Error; err != nil {
		t.Fatalf("load client: %v", err)
	}
	if got.IPv4 != "61.224.64.204" {
		t.Fatalf("agent-owned IPv4 was not updated: %q", got.IPv4)
	}
	if got.Name != "managed name" || got.PublicRemark != "managed remark" || !got.Hidden {
		t.Fatalf("administrator-owned metadata was overwritten: %+v", got)
	}
	if got.Region == "forged region" {
		t.Fatal("agent-supplied region was trusted")
	}
	if len(got.ReachableAddresses) != 1 || got.ReachableAddresses[0] != "141.11.86.162" {
		t.Fatalf("reachable addresses were overwritten: %#v", got.ReachableAddresses)
	}
}
