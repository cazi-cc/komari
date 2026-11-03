// Package trafficledger keeps dashboard lifetime totals separate from agent
// counters, which may reset on reboot or at a monthly billing boundary.
package trafficledger

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

type Entry struct {
	TotalUp     int64     `json:"total_up"`
	TotalDown   int64     `json:"total_down"`
	CounterUp   int64     `json:"counter_up"`
	CounterDown int64     `json:"counter_down"`
	LastReport  time.Time `json:"last_report"`
}

type document struct {
	Version int              `json:"version"`
	Nodes   map[string]Entry `json:"nodes"`
}

type Ledger struct {
	mu          sync.Mutex
	path        string
	nodes       map[string]Entry
	lastAttempt time.Time
	dirty       bool
}

var current atomic.Pointer[Ledger]

func Open(path string) (*Ledger, error) {
	l := &Ledger{path: path, nodes: make(map[string]Entry), lastAttempt: time.Now()}
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return l, nil
	}
	if err != nil {
		return nil, err
	}
	var saved document
	if err := json.Unmarshal(data, &saved); err != nil {
		return nil, fmt.Errorf("invalid lifetime traffic ledger: %w", err)
	}
	if saved.Version != 1 || saved.Nodes == nil {
		return nil, fmt.Errorf("unsupported lifetime traffic ledger")
	}
	for uuid, entry := range saved.Nodes {
		if uuid == "" || entry.TotalUp < entry.CounterUp || entry.TotalDown < entry.CounterDown || entry.CounterUp < 0 || entry.CounterDown < 0 {
			return nil, fmt.Errorf("invalid lifetime traffic entry")
		}
	}
	l.nodes = saved.Nodes
	return l, nil
}

func SetDefault(l *Ledger) { current.Store(l) }

func Observe(uuid string, timestamp time.Time, up, down int64) error {
	if l := current.Load(); l != nil {
		return l.Observe(uuid, timestamp, up, down)
	}
	return nil
}

func Totals(uuid string, fallbackUp, fallbackDown int64) (int64, int64) {
	if l := current.Load(); l != nil {
		if entry, ok := l.Get(uuid); ok {
			return entry.TotalUp, entry.TotalDown
		}
	}
	return fallbackUp, fallbackDown
}

func counterDelta(now, before int64) int64 {
	if now >= before {
		return now - before
	}
	return now
}

func add(total, delta int64) int64 {
	if delta > math.MaxInt64-total {
		return math.MaxInt64
	}
	return total + delta
}

func (l *Ledger) Observe(uuid string, timestamp time.Time, up, down int64) error {
	if uuid == "" || timestamp.IsZero() || up < 0 || down < 0 {
		return fmt.Errorf("invalid lifetime traffic observation")
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.nodes[uuid]
	if ok && !timestamp.After(entry.LastReport) {
		return nil
	}
	if !ok {
		entry.TotalUp, entry.TotalDown = up, down
	} else {
		entry.TotalUp = add(entry.TotalUp, counterDelta(up, entry.CounterUp))
		entry.TotalDown = add(entry.TotalDown, counterDelta(down, entry.CounterDown))
	}
	entry.CounterUp, entry.CounterDown = up, down
	entry.LastReport = timestamp.UTC()
	l.nodes[uuid] = entry
	l.dirty = true
	// Only tiny state changes happen per report; disk writes are batched once
	// per minute, independently of visitor count and metric retention.
	if time.Since(l.lastAttempt) >= time.Minute {
		return l.saveLocked()
	}
	return nil
}

func (l *Ledger) Get(uuid string) (Entry, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	entry, ok := l.nodes[uuid]
	return entry, ok
}

func (l *Ledger) Save() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.saveLocked()
}

func (l *Ledger) Close() error {
	current.CompareAndSwap(l, nil)
	return l.Save()
}

func (l *Ledger) saveLocked() error {
	l.lastAttempt = time.Now()
	if !l.dirty {
		return nil
	}
	data, err := json.Marshal(document{Version: 1, Nodes: l.nodes})
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(l.path), 0700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(l.path), ".traffic-lifetime-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err = tmp.Write(data); err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	if err := os.Rename(tmp.Name(), l.path); err != nil {
		return err
	}
	l.dirty = false
	return nil
}
