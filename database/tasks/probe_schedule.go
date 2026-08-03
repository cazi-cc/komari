package tasks

import (
	"fmt"
	"hash/fnv"
	"math"
	"sort"
	"sync"

	"github.com/komari-monitor/komari/database/dbcore"
	"github.com/komari-monitor/komari/database/models"
	"github.com/komari-monitor/komari/utils"
	"gorm.io/gorm"
)

const maximumProbePhaseCandidates = 900

var probeScheduleMu sync.Mutex

type scheduledProbe struct {
	key        string
	family     string
	id         uint
	lane       string
	intervalMS int64
	durationMS int64
	clients    []string
	phaseMS    *int64
	phaseFor   *int
}

// ReloadProbeSchedules plans both light and heavy probes together. Phases are
// persisted and anchored to UTC epoch time, so unrelated edits and restarts do
// not restart every task from the current wall clock.
func ReloadProbeSchedules() error {
	probeScheduleMu.Lock()
	defer probeScheduleMu.Unlock()

	pingTasks, err := GetAllPingTasks()
	if err != nil {
		return err
	}
	tcpTasks, err := GetAllTCPQualityTasks()
	if err != nil {
		return err
	}
	unlockTasks, err := GetAllUnlockQualityTasks()
	if err != nil {
		return err
	}
	if err := ensureProbeSchedulePhases(pingTasks, tcpTasks, unlockTasks); err != nil {
		return err
	}
	if err := utils.ReloadPingSchedule(pingTasks); err != nil {
		return err
	}
	if err := utils.ReloadTCPQualitySchedule(tcpTasks); err != nil {
		return err
	}
	return utils.ReloadUnlockQualitySchedule(unlockTasks)
}

func ensureProbeSchedulePhases(pingTasks []models.PingTask, tcpTasks []models.TCPQualityTask, unlockTasks []models.UnlockQualityTask) error {
	items := make([]scheduledProbe, 0, len(pingTasks)+len(tcpTasks)+len(unlockTasks))
	for index := range pingTasks {
		task := &pingTasks[index]
		if task.Interval <= 0 {
			continue
		}
		items = append(items, scheduledProbe{
			key: fmt.Sprintf("ping:%d", task.Id), family: "ping", id: task.Id, lane: "probe",
			intervalMS: int64(task.Interval) * 1000, durationMS: pingProbeDurationMS(*task),
			clients: task.Clients, phaseMS: &task.SchedulePhaseMS, phaseFor: &task.ScheduleInterval,
		})
	}
	for index := range tcpTasks {
		task := &tcpTasks[index]
		if !task.Enabled || task.Interval <= 0 {
			continue
		}
		items = append(items, scheduledProbe{
			key: fmt.Sprintf("tcp-quality:%d", task.Id), family: "tcp-quality", id: task.Id, lane: "probe",
			intervalMS: int64(task.Interval) * 1000, durationMS: tcpQualityDurationMS(*task),
			clients: task.Clients, phaseMS: &task.SchedulePhaseMS, phaseFor: &task.ScheduleInterval,
		})
	}
	for index := range unlockTasks {
		task := &unlockTasks[index]
		if !task.Enabled || task.Interval <= 0 {
			continue
		}
		items = append(items, scheduledProbe{
			key: fmt.Sprintf("unlock-quality:%d", task.Id), family: "unlock-quality", id: task.Id, lane: "probe",
			intervalMS: int64(task.Interval) * 1000, durationMS: unlockQualityDurationMS(*task),
			clients: task.Clients, phaseMS: &task.SchedulePhaseMS, phaseFor: &task.ScheduleInterval,
		})
	}

	placed := make([]scheduledProbe, 0, len(items))
	pending := make([]scheduledProbe, 0, len(items))
	for _, item := range items {
		if validStoredProbePhase(item) {
			placed = append(placed, item)
		} else {
			pending = append(pending, item)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		if pending[i].lane != pending[j].lane {
			return pending[i].lane < pending[j].lane
		}
		if pending[i].intervalMS != pending[j].intervalMS {
			return pending[i].intervalMS < pending[j].intervalMS
		}
		return pending[i].key < pending[j].key
	})
	for index := range pending {
		item := &pending[index]
		*item.phaseMS = chooseProbePhase(*item, placed)
		*item.phaseFor = int(item.intervalMS / 1000)
		placed = append(placed, *item)
	}
	if len(pending) == 0 {
		return nil
	}

	return dbcore.GetDBInstance().Transaction(func(tx *gorm.DB) error {
		for _, item := range pending {
			var model any
			switch item.family {
			case "ping":
				model = &models.PingTask{}
			case "tcp-quality":
				model = &models.TCPQualityTask{}
			case "unlock-quality":
				model = &models.UnlockQualityTask{}
			default:
				continue
			}
			if err := tx.Model(model).Where("id = ?", item.id).Updates(map[string]any{
				"schedule_phase_ms": *item.phaseMS,
				"schedule_interval": *item.phaseFor,
			}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func validStoredProbePhase(item scheduledProbe) bool {
	return item.phaseMS != nil && item.phaseFor != nil && *item.phaseFor == int(item.intervalMS/1000) && *item.phaseMS >= 0 && *item.phaseMS < item.intervalMS
}

func chooseProbePhase(item scheduledProbe, placed []scheduledProbe) int64 {
	period := item.intervalMS
	candidateCount := int(period / 1000)
	if candidateCount < 1 {
		candidateCount = 1
	}
	if candidateCount > maximumProbePhaseCandidates {
		candidateCount = maximumProbePhaseCandidates
	}
	seed := stableScheduleHash(item.key) % uint64(period)
	bestPhase := int64(seed)
	bestMinimum := -1.0
	bestPenalty := math.Inf(1)
	for index := 0; index < candidateCount; index++ {
		candidate := probePhaseCandidate(item.key, period, int64(seed), index, candidateCount)
		minimum := math.Inf(1)
		penalty := 0.0
		matched := false
		for _, other := range placed {
			if item.lane != other.lane {
				continue
			}
			overlap := overlappingProbeClients(item.clients, other.clients)
			if overlap == 0 {
				continue
			}
			matched = true
			cycle := gcd64(period, other.intervalMS)
			distance := circularDistance(candidate%cycle, *other.phaseMS%cycle, cycle)
			guard := float64(max64(250, (item.durationMS+other.durationMS)/2))
			normalized := float64(distance) / guard
			if normalized < minimum {
				minimum = normalized
			}
			penalty += float64(overlap) / math.Max(0.25, normalized)
		}
		if !matched {
			minimum = math.Inf(1)
		}
		if minimum > bestMinimum || (minimum == bestMinimum && penalty < bestPenalty) {
			bestPhase, bestMinimum, bestPenalty = candidate, minimum, penalty
		}
	}
	return bestPhase
}

func probePhaseCandidate(key string, period, seed int64, index, count int) int64 {
	if index == 0 || count <= 1 {
		return seed
	}
	bucketStart := int64(index) * period / int64(count)
	bucketEnd := int64(index+1) * period / int64(count)
	bucketWidth := max64(1, bucketEnd-bucketStart)
	jitter := stableScheduleHash(fmt.Sprintf("%s:candidate:%d", key, index)) % uint64(bucketWidth)
	return (seed + bucketStart + int64(jitter)) % period
}

func overlappingProbeClients(left, right []string) int {
	seen := make(map[string]struct{}, len(left))
	for _, client := range left {
		if client != "" {
			seen[client] = struct{}{}
		}
	}
	count := 0
	for _, client := range right {
		if _, exists := seen[client]; exists {
			count++
		}
	}
	return count
}

func pingProbeDurationMS(task models.PingTask) int64 {
	samples := task.ProbeConfig.SampleCount
	if samples < 1 {
		samples = 1
	}
	if task.Type == "icmp" {
		return max64(500, int64(samples)*250)
	}
	timeout := task.ProbeConfig.TimeoutMS
	if timeout <= 0 {
		timeout = 3000
	}
	return min64(int64(timeout), 5000)
}

func tcpQualityDurationMS(task models.TCPQualityTask) int64 {
	targets := len(task.ProvinceCodes) * len(task.ISPCode) * len(task.IPVersions)
	packets := targets * task.StandardPackets
	if task.LargeEnabled {
		packets += targets * task.LargePackets
	}
	return max64(1000, int64(packets*probeMaxInt(task.DelayMS, 50)/4))
}

func unlockQualityDurationMS(task models.UnlockQualityTask) int64 {
	routes := 1
	if task.ControlEnabled {
		routes++
	}
	if task.FixedEnabled {
		routes++
	}
	return max64(2000, int64(routes*probeMaxInt(task.SampleCount, 1)*2000))
}

func stableScheduleHash(value string) uint64 {
	hasher := fnv.New64a()
	_, _ = hasher.Write([]byte(value))
	return hasher.Sum64()
}

func gcd64(left, right int64) int64 {
	for right != 0 {
		left, right = right, left%right
	}
	if left < 0 {
		return -left
	}
	return left
}

func circularDistance(left, right, period int64) int64 {
	distance := left - right
	if distance < 0 {
		distance = -distance
	}
	if alternate := period - distance; alternate < distance {
		return alternate
	}
	return distance
}

func min64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}

func max64(left, right int64) int64 {
	if left > right {
		return left
	}
	return right
}

func probeMaxInt(left, right int) int {
	if left > right {
		return left
	}
	return right
}
