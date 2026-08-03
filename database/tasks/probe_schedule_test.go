package tasks

import "testing"

func TestChooseProbePhaseUsesLargestSharedClientGap(t *testing.T) {
	firstPhase := int64(0)
	secondPhase := int64(30000)
	placed := []scheduledProbe{
		{key: "ping:1", lane: "probe", intervalMS: 60000, durationMS: 500, clients: []string{"node-a"}, phaseMS: &firstPhase},
		{key: "ping:2", lane: "probe", intervalMS: 60000, durationMS: 500, clients: []string{"node-a"}, phaseMS: &secondPhase},
	}
	phase := chooseProbePhase(scheduledProbe{
		key: "ping:3", lane: "probe", intervalMS: 60000, durationMS: 500, clients: []string{"node-a"},
	}, placed)
	left := circularDistance(phase, firstPhase, 60000)
	right := circularDistance(phase, secondPhase, 60000)
	if left < 14000 || right < 14000 {
		t.Fatalf("phase %d is not near the center of a free gap", phase)
	}
}

func TestChooseProbePhaseCoordinatesHeavyFamilies(t *testing.T) {
	unlockPhase := int64(10000)
	phase := chooseProbePhase(scheduledProbe{
		key: "tcp-quality:4", lane: "probe", intervalMS: 60000, durationMS: 3000, clients: []string{"node-a"},
	}, []scheduledProbe{{
		key: "unlock-quality:1", lane: "probe", intervalMS: 60000, durationMS: 3000, clients: []string{"node-a"}, phaseMS: &unlockPhase,
	}})
	if distance := circularDistance(phase, unlockPhase, 60000); distance < 29000 {
		t.Fatalf("heavy task distance = %dms, want about half an interval", distance)
	}
}

func TestChooseProbePhaseCoordinatesAllClientTasks(t *testing.T) {
	occupied := int64(10000)
	phase := chooseProbePhase(scheduledProbe{
		key: "tcp-quality:all", lane: "probe", intervalMS: 60000,
		durationMS: 3000, allClients: true,
	}, []scheduledProbe{{
		key: "ping:all", lane: "probe", intervalMS: 60000,
		durationMS: 500, allClients: true, phaseMS: &occupied,
	}})
	if distance := circularDistance(phase, occupied, 60000); distance < 29000 {
		t.Fatalf("all-client task distance = %dms, want about half an interval", distance)
	}
}

func TestChooseProbePhaseCoordinatesAllClientAndExplicitTask(t *testing.T) {
	occupied := int64(10000)
	phase := chooseProbePhase(scheduledProbe{
		key: "unlock-quality:all", lane: "probe", intervalMS: 60000,
		durationMS: 3000, allClients: true,
	}, []scheduledProbe{{
		key: "ping:node-a", lane: "probe", intervalMS: 60000,
		durationMS: 500, clients: []string{"node-a"}, phaseMS: &occupied,
	}})
	if distance := circularDistance(phase, occupied, 60000); distance < 29000 {
		t.Fatalf("all-client to explicit distance = %dms, want about half an interval", distance)
	}
}

func TestChooseProbePhaseVariesSubsecondOffset(t *testing.T) {
	item := scheduledProbe{
		key: "tcp-quality:subsecond", lane: "probe", intervalMS: 60000,
		durationMS: 3000, clients: []string{"node-a"},
	}
	seedRemainder := int64(stableScheduleHash(item.key) % 1000)
	occupied := (seedRemainder + 50) % 1000
	phase := chooseProbePhase(item, []scheduledProbe{{
		key: "ping:short-cycle", lane: "probe", intervalMS: 1000,
		durationMS: 500, clients: []string{"node-a"}, phaseMS: &occupied,
	}})
	if distance := circularDistance(phase%1000, occupied, 1000); distance < 250 {
		t.Fatalf("subsecond distance = %dms, want at least the 250ms guard", distance)
	}
}

func TestChooseProbePhaseIgnoresUnrelatedClients(t *testing.T) {
	occupied := int64(0)
	item := scheduledProbe{key: "ping:9", lane: "probe", intervalMS: 60000, durationMS: 500, clients: []string{"node-b"}}
	phase := chooseProbePhase(item, []scheduledProbe{{
		key: "ping:1", lane: "probe", intervalMS: 60000, durationMS: 500, clients: []string{"node-a"}, phaseMS: &occupied,
	}})
	want := int64(stableScheduleHash(item.key) % uint64(item.intervalMS))
	if phase != want {
		t.Fatalf("phase = %d, want stable phase %d for unrelated clients", phase, want)
	}
}

func TestNewAndDeletedTasksDoNotMoveStoredPhases(t *testing.T) {
	stored := int64(17000)
	storedInterval := 60
	existing := scheduledProbe{
		key: "ping:1", lane: "probe", intervalMS: 60000, durationMS: 500,
		clients: []string{"node-a"}, phaseMS: &stored, phaseFor: &storedInterval,
	}
	if !validStoredProbePhase(existing) {
		t.Fatal("existing phase should remain valid")
	}
	newPhase := chooseProbePhase(scheduledProbe{
		key: "tcp-quality:2", lane: "probe", intervalMS: 60000, durationMS: 3000, clients: []string{"node-a"},
	}, []scheduledProbe{existing})
	if stored != 17000 {
		t.Fatalf("adding a task moved existing phase to %d", stored)
	}
	if circularDistance(newPhase, stored, 60000) < 29000 {
		t.Fatalf("new cross-family task phase %d was not placed away from stored phase %d", newPhase, stored)
	}
	if !validStoredProbePhase(existing) || stored != 17000 {
		t.Fatal("deleting another task must not invalidate or move the stored phase")
	}
}
