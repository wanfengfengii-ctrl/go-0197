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

// TestValidateFactsAcceptsConsistentSet verifies that a fully consistent
// bootstrap facts set passes validation.
func TestValidateFactsAcceptsConsistentSet(t *testing.T) {
	specs := []Specimen{{ID: "S1"}, {ID: "S2"}}
	batches := []BatchRevision{
		{SampleID: "S1", BatchID: "B1", Revision: "r1"},
		{SampleID: "S2", BatchID: "B1", Revision: "r1"},
	}
	mothers := []Container{
		{ID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1"},
		{ID: "M2", SampleID: "S2", BatchID: "B1", Revision: "r1"},
	}
	if err := ValidateFacts(specs, batches, mothers); err != nil {
		t.Fatalf("expected consistent facts to pass, got %v", err)
	}
}

// TestValidateFactsRejectsMissingSpecimen verifies that a batch revision
// referencing an unregistered specimen is rejected.
func TestValidateFactsRejectsMissingSpecimen(t *testing.T) {
	specs := []Specimen{{ID: "S1"}}
	batches := []BatchRevision{{SampleID: "S2", BatchID: "B1", Revision: "r1"}}
	mothers := []Container{{ID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1"}}
	if err := ValidateFacts(specs, batches, mothers); err == nil {
		t.Fatalf("expected batch revision with unknown specimen to be rejected")
	}
}

// TestValidateFactsRejectsMotherSampleMismatch verifies that a mother whose
// sample does not match any registered specimen is rejected.
func TestValidateFactsRejectsMotherSampleMismatch(t *testing.T) {
	specs := []Specimen{{ID: "S1"}}
	batches := []BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}}
	mothers := []Container{{ID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1"}}
	if err := ValidateFacts(specs, batches, mothers); err == nil {
		t.Fatalf("expected mother sample mismatch to be rejected")
	}
}

// TestValidateFactsRejectsMissingBatchRevision verifies that a mother
// referencing a batch revision that was not registered is rejected.
func TestValidateFactsRejectsMissingBatchRevision(t *testing.T) {
	specs := []Specimen{{ID: "S1"}}
	batches := []BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r2"}}
	mothers := []Container{{ID: "M1", SampleID: "S1", BatchID: "B1", Revision: "r1"}}
	if err := ValidateFacts(specs, batches, mothers); err == nil {
		t.Fatalf("expected mother referencing unknown batch revision to be rejected")
	}
}

// TestValidateFactsRejectsCrossSampleBatch verifies that a mother using a batch
// revision that belongs to another sample is rejected.
func TestValidateFactsRejectsCrossSampleBatch(t *testing.T) {
	specs := []Specimen{{ID: "S1"}, {ID: "S2"}}
	batches := []BatchRevision{{SampleID: "S1", BatchID: "B1", Revision: "r1"}}
	mothers := []Container{{ID: "M1", SampleID: "S2", BatchID: "B1", Revision: "r1"}}
	if err := ValidateFacts(specs, batches, mothers); err == nil {
		t.Fatalf("expected cross-sample batch reference to be rejected")
	}
}
