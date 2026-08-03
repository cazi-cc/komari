package utils

import (
	"fmt"
	"hash/fnv"
	"sort"
	"time"
)

const minimumProbeStartDelay = 100 * time.Millisecond

// probeScheduleDelays assigns stable, evenly spaced phases to tasks sharing an
// interval. Operators can therefore use conventional values such as 30 or 60
// seconds without causing every task to start together after a schedule reload.
func probeScheduleDelays(namespace string, intervalSeconds int, taskIDs []uint) map[uint]time.Duration {
	result := make(map[uint]time.Duration, len(taskIDs))
	if intervalSeconds <= 0 || len(taskIDs) == 0 {
		return result
	}

	unique := make(map[uint]struct{}, len(taskIDs))
	ids := make([]uint, 0, len(taskIDs))
	for _, id := range taskIDs {
		if id == 0 {
			continue
		}
		if _, exists := unique[id]; exists {
			continue
		}
		unique[id] = struct{}{}
		ids = append(ids, id)
	}
	if len(ids) == 0 {
		return result
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })

	period := time.Duration(intervalSeconds) * time.Second
	base := stableProbePhase(fmt.Sprintf("%s:%d", namespace, intervalSeconds), period)
	count := time.Duration(len(ids))
	for index, id := range ids {
		// Put each task in the center of its slice, then rotate the whole group
		// by a stable namespace-specific phase to avoid cross-family alignment.
		center := period * time.Duration(2*index+1) / (2 * count)
		delay := (base + center) % period
		if delay < minimumProbeStartDelay && period > minimumProbeStartDelay {
			delay = minimumProbeStartDelay
		}
		result[id] = delay
	}
	return result
}

func stableProbePhase(key string, period time.Duration) time.Duration {
	if period <= 0 {
		return 0
	}
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(key))
	return time.Duration(hasher.Sum64() % uint64(period))
}
