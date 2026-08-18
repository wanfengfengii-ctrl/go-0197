package volume

import "testing"

func TestCheckConservationBalanced(t *testing.T) {
	// 1200 locked = 500 + 400 + 200 allocated + 100 loss + 0 remaining.
	if err := CheckConservation(1200, 1100, 100, 0, 0); err != nil {
		t.Fatalf("expected balanced ledger, got %v", err)
	}
}

func TestCheckConservationRejectsImbalance(t *testing.T) {
	if err := CheckConservation(1200, 1000, 100, 0, 0); err == nil {
		t.Fatalf("expected imbalance to be rejected")
	}
}

func TestCheckConservationRejectsOutstandingPlanOverRemaining(t *testing.T) {
	// 1200 locked = 600 allocated + 0 loss + 600 remaining, but 600 outstanding.
	if err := CheckConservation(1200, 600, 0, 600, 700); err == nil {
		t.Fatalf("expected outstanding plan exceeding remaining to be rejected")
	}
}

func TestCheckConservationRejectsNegativeVolume(t *testing.T) {
	if err := CheckConservation(-1, 0, 0, 0, 0); err == nil {
		t.Fatalf("expected negative volume to be rejected")
	}
}
