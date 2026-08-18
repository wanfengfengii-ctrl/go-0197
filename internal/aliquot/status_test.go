package aliquot

import "testing"

func TestMotherStatusTransitions(t *testing.T) {
	allowed := []struct {
		from, to MotherStatus
	}{
		{StatusAvailable, StatusReserved},
		{StatusReserved, StatusThawed},
		{StatusThawed, StatusAliquoting},
		{StatusAliquoting, StatusDepleted},
		{StatusDepleted, StatusFinalized},
		{StatusReserved, StatusQuarantined},
		{StatusThawed, StatusQuarantined},
		{StatusAliquoting, StatusQuarantined},
		{StatusDepleted, StatusQuarantined},
	}
	for _, tc := range allowed {
		if !CanTransition(tc.from, tc.to) {
			t.Errorf("expected %s -> %s to be allowed", tc.from, tc.to)
		}
	}
}

func TestTerminalStatesAreIrreversible(t *testing.T) {
	for _, term := range []MotherStatus{StatusFinalized, StatusQuarantined} {
		if !term.IsTerminal() {
			t.Errorf("expected %s to be terminal", term)
		}
		for _, next := range []MotherStatus{StatusAvailable, StatusReserved, StatusThawed, StatusAliquoting, StatusDepleted, StatusFinalized, StatusQuarantined} {
			if CanTransition(term, next) {
				t.Errorf("terminal state %s must not transition to %s", term, next)
			}
		}
	}
}

func TestSnapshotPlannedVolume(t *testing.T) {
	s := Snapshot{
		LockedVolumeUL: 1200,
		Children: []ChildPlan{
			{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: 500},
			{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: 400},
			{Ordinal: 2, ChildTubeID: "C3", PlannedVolumeUL: 200},
		},
	}
	if got := s.PlannedVolume(); got != 1100 {
		t.Fatalf("PlannedVolume = %d, want 1100", got)
	}
}
