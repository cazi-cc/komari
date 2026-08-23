package clients

import (
	"reflect"
	"testing"

	"github.com/komari-monitor/komari/database/models"
)

func TestNormalizeReachableAddresses(t *testing.T) {
	got, err := normalizeReachableAddresses([]interface{}{
		" 141.11.86.162 ",
		"[2001:db8::1]",
		"141.11.86.162",
		"",
	})
	if err != nil {
		t.Fatalf("normalize reachable addresses: %v", err)
	}
	want := models.StringArray{"141.11.86.162", "2001:db8::1"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized addresses = %#v, want %#v", got, want)
	}
}

func TestNormalizeReachableAddressesRejectsInvalidValue(t *testing.T) {
	if _, err := normalizeReachableAddresses([]interface{}{"example.com"}); err == nil {
		t.Fatal("invalid reachable address was accepted")
	}
}

func TestNormalizeReachableAddressesRejectsTooManyValues(t *testing.T) {
	values := make([]string, maxReachableAddressesPerClient+1)
	for index := range values {
		values[index] = "192.0.2.1"
	}
	if _, err := normalizeReachableAddresses(values); err == nil {
		t.Fatal("too many reachable addresses were accepted")
	}
}
