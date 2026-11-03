package trafficledger

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestMonthlyResetDoesNotResetLifetime(t *testing.T) {
	l, err := Open(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	l.nodes["node"] = Entry{TotalUp: 1000, TotalDown: 2000, CounterUp: 100, CounterDown: 200, LastReport: base}
	for i, pair := range [][2]int64{{110, 230}, {2, 3}, {7, 8}} {
		if err := l.Observe("node", base.Add(time.Duration(i+1)*time.Second), pair[0], pair[1]); err != nil {
			t.Fatal(err)
		}
	}
	entry, _ := l.Get("node")
	if entry.TotalUp != 1017 || entry.TotalDown != 2038 {
		t.Fatalf("totals after monthly reset: %+v", entry)
	}
	if entry.CounterUp != 7 || entry.CounterDown != 8 {
		t.Fatalf("billing counters changed: %+v", entry)
	}
	if err := l.Save(); err != nil {
		t.Fatal(err)
	}
	restored, err := Open(l.path)
	if err != nil {
		t.Fatal(err)
	}
	if err := restored.Observe("node", base.Add(4*time.Second), 12, 18); err != nil {
		t.Fatal(err)
	}
	entry, _ = restored.Get("node")
	if entry.TotalUp != 1022 || entry.TotalDown != 2048 {
		t.Fatalf("restart lost or duplicated traffic: %+v", entry)
	}
}

func TestDuplicatesAndOldReportsDoNotDoubleCount(t *testing.T) {
	l, _ := Open(filepath.Join(t.TempDir(), "ledger.json"))
	base := time.Now().UTC()
	for _, observation := range []struct {
		offset time.Duration
		up     int64
	}{{0, 100}, {time.Second, 150}, {time.Second, 150}, {0, 100}, {2 * time.Second, 0}, {3 * time.Second, 20}} {
		if err := l.Observe("node", base.Add(observation.offset), observation.up, observation.up); err != nil {
			t.Fatal(err)
		}
	}
	entry, _ := l.Get("node")
	if entry.TotalUp != 170 || entry.TotalDown != 170 {
		t.Fatalf("unexpected total %+v", entry)
	}
}

func TestCorruptLedgerIsNotSilentlyReset(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(path); err == nil {
		t.Fatal("corruption accepted")
	}
	data, _ := os.ReadFile(path)
	if string(data) != "broken" {
		t.Fatal("corrupt evidence was overwritten")
	}
}

func TestConcurrentNodesAndBatchedPersistence(t *testing.T) {
	l, _ := Open(filepath.Join(t.TempDir(), "ledger.json"))
	var wg sync.WaitGroup
	for _, name := range []string{"one", "two", "three"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			base := time.Now().UTC()
			for i := 1; i <= 100; i++ {
				if err := l.Observe(name, base.Add(time.Duration(i)*time.Second), int64(i), int64(i)); err != nil {
					t.Error(err)
				}
			}
		}(name)
	}
	wg.Wait()
	if _, err := os.Stat(l.path); !os.IsNotExist(err) {
		t.Fatal("every report unexpectedly wrote to disk")
	}
	if err := l.Save(); err != nil {
		t.Fatal(err)
	}
	stat, _ := os.Stat(l.path)
	if stat.Mode().Perm() != 0600 {
		t.Fatalf("unsafe ledger permissions: %v", stat.Mode())
	}
	for _, name := range []string{"one", "two", "three"} {
		entry, ok := l.Get(name)
		if !ok || entry.TotalUp != 100 {
			t.Fatalf("node %s lost traffic: %+v", name, entry)
		}
	}
}
