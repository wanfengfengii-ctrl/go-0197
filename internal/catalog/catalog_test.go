package catalog

import "testing"

func TestValidateReferenceAcceptsMatchingTriple(t *testing.T) {
	s := Specimen{ID: "S1", Description: "sample one"}
	b := BatchRevision{SampleID: "S1", BatchID: "B1", Revision: "r1"}
	c := Container{ID: "M1", Type: MotherContainer, SampleID: "S1", BatchID: "B1", Revision: "r1", RemainingUL: 1200}

	if err := ValidateReference(s, b, c); err != nil {
		t.Fatalf("expected match, got %v", err)
	}
	if !c.Matches(s, b) {
		t.Fatalf("Matches returned false for consistent triple")
	}
}

func TestValidateReferenceRejectsSampleMismatch(t *testing.T) {
	s := Specimen{ID: "S1"}
	b := BatchRevision{SampleID: "S1", BatchID: "B1", Revision: "r1"}
	c := Container{ID: "M1", Type: MotherContainer, SampleID: "S2", BatchID: "B1", Revision: "r1"}

	if err := ValidateReference(s, b, c); err == nil {
		t.Fatalf("expected sample mismatch to be rejected")
	}
}

func TestValidateReferenceRejectsBatchMismatch(t *testing.T) {
	s := Specimen{ID: "S1"}
	b := BatchRevision{SampleID: "S1", BatchID: "B1", Revision: "r1"}
	c := Container{ID: "M1", Type: MotherContainer, SampleID: "S1", BatchID: "B2", Revision: "r1"}

	if err := ValidateReference(s, b, c); err == nil {
		t.Fatalf("expected batch mismatch to be rejected")
	}
}
