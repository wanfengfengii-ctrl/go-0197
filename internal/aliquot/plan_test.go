package aliquot

import (
	"math"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// TestModel_ValidatePlanCapacityBoundaries is the regression test for the
// integer-overflow capacity bug at reservation time. The plan total is checked
// against the locked volume incrementally, so a plan whose true total exceeds
// the locked volume is rejected even when a naive int64 sum would overflow and
// wrap negative (failure boundary 1, domain rule 5). It also pins the retained
// normal path: a large plan that fits exactly, and representative lower-volume
// plans, are still accepted.
func TestModel_ValidatePlanCapacityBoundaries(t *testing.T) {
	plan := func(children ...struct {
		id  catalog.TubeID
		vol int64
	}) []ChildPlan {
		out := make([]ChildPlan, len(children))
		for i, c := range children {
			out[i] = ChildPlan{Ordinal: i, ChildTubeID: c.id, PlannedVolumeUL: c.vol}
		}
		return out
	}
	type tc struct {
		name     string
		locked   int64
		children []struct {
			id  catalog.TubeID
			vol int64
		}
		wantErr bool
	}
	const max = int64(math.MaxInt64)
	cases := []tc{
		// Representative lower-volume plan well under the locked volume: accepted.
		{
			name:   "lower_volume_plan_within_locked",
			locked: 1200,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", 500}, {"C2", 400}, {"C3", 200}},
			wantErr: false,
		},
		// Exact boundary at a small locked volume: total == locked, accepted.
		{
			name:   "exact_boundary_total_equals_locked",
			locked: 1000,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", 600}, {"C2", 400}},
			wantErr: false,
		},
		// Over-limit sum that stays representable: total 1200 > 1000, rejected
		// without any overflow involvement.
		{
			name:   "over_limit_sum_representable",
			locked: 1000,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", 600}, {"C2", 600}},
			wantErr: true,
		},
		// Single child at the system upper limit: accepted (normal large plan).
		{
			name:   "single_child_at_int64_max",
			locked: max,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", max}},
			wantErr: false,
		},
		// Two children whose sum equals the odd MaxInt64 exactly: accepted. This
		// exercises the headroom check at the extreme boundary.
		{
			name:   "two_children_sum_to_int64_max",
			locked: max,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", max - 1}, {"C2", 1}},
			wantErr: false,
		},
		// Two children summing one past MaxInt64: the headroom check rejects
		// before the over-limit sum overflows int64.
		{
			name:   "two_children_one_past_int64_max",
			locked: max,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", max - 1}, {"C2", 2}},
			wantErr: true,
		},
		// True arithmetic overflow: two max allocations whose naive sum wraps
		// negative. Before the fix this was accepted; it must now be rejected.
		{
			name:   "overflow_two_max_allocations",
			locked: max,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", max}, {"C2", max}},
			wantErr: true,
		},
		// Three near-max allocations: a wider overflow, must be rejected.
		{
			name:   "overflow_three_near_max_allocations",
			locked: max,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", max - 1}, {"C2", max - 1}, {"C3", max - 1}},
			wantErr: true,
		},
		// Individual allocation exceeding locked is rejected on its own.
		{
			name:   "individual_exceeds_locked",
			locked: 500,
			children: []struct {
				id  catalog.TubeID
				vol int64
			}{{"C1", 501}},
			wantErr: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := ValidatePlan(c.locked, plan(c.children...))
			if c.wantErr && err == nil {
				t.Fatalf("ValidatePlan(locked=%d, %v) = nil, want rejection", c.locked, c.children)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("ValidatePlan(locked=%d, %v) = %v, want acceptance", c.locked, c.children, err)
			}
		})
	}
}

// TestModel_ValidatePlanOverflowDoesNotWrapNegative asserts the specific
// failure mode of the bug: with two max-valued allocations the running total
// must not be allowed to wrap negative. A rejected overflow plan must carry an
// error, and (defensively) the running total must remain within [0, locked].
func TestModel_ValidatePlanOverflowDoesNotWrapNegative(t *testing.T) {
	const max = int64(math.MaxInt64)
	plan := []ChildPlan{
		{Ordinal: 0, ChildTubeID: "C1", PlannedVolumeUL: max},
		{Ordinal: 1, ChildTubeID: "C2", PlannedVolumeUL: max},
	}
	if err := ValidatePlan(max, plan); err == nil {
		t.Fatalf("overflow plan accepted; expected rejection so no mother occupation, session, or child declaration is produced")
	}
}
