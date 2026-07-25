package server

import (
	"os"
	"reflect"
	"testing"
)

func TestTrustedReverseProxies(t *testing.T) {
	original, existed := os.LookupEnv("KOMARI_TRUSTED_PROXIES")
	t.Cleanup(func() {
		if existed {
			_ = os.Setenv("KOMARI_TRUSTED_PROXIES", original)
		} else {
			_ = os.Unsetenv("KOMARI_TRUSTED_PROXIES")
		}
	})

	_ = os.Unsetenv("KOMARI_TRUSTED_PROXIES")
	if got := trustedReverseProxies(); !reflect.DeepEqual(got, []string{"127.0.0.1", "::1"}) {
		t.Fatalf("unexpected defaults: %v", got)
	}

	_ = os.Setenv("KOMARI_TRUSTED_PROXIES", "127.0.0.1, ::1; 172.20.0.1")
	if got := trustedReverseProxies(); !reflect.DeepEqual(got, []string{"127.0.0.1", "::1", "172.20.0.1"}) {
		t.Fatalf("unexpected configured proxies: %v", got)
	}
}
