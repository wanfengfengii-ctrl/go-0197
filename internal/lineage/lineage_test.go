package lineage

import (
	"reflect"
	"testing"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

func TestMissingVerificationsStableOrder(t *testing.T) {
	plan := []catalog.TubeID{"C2", "C1", "C3"}
	verifications := map[catalog.TubeID]Verification{
		"C1": {ChildTubeID: "C1"},
	}
	got := MissingVerifications(plan, verifications)
	want := []catalog.TubeID{"C2", "C3"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("MissingVerifications = %v, want %v", got, want)
	}
}

func TestMissingVerificationsNone(t *testing.T) {
	plan := []catalog.TubeID{"C1", "C2"}
	verifications := map[catalog.TubeID]Verification{
		"C1": {ChildTubeID: "C1"},
		"C2": {ChildTubeID: "C2"},
	}
	if got := MissingVerifications(plan, verifications); len(got) != 0 {
		t.Fatalf("expected no missing verifications, got %v", got)
	}
}
