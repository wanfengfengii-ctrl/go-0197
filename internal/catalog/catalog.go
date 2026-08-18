// Package catalog defines the immutable identity types for the sample and
// container catalog: specimens, append-only batch revisions, and mother/child
// tubes. It centralizes validation of catalog references, container types,
// batch matching, and global tube-number availability (domain rules 1-4).
package catalog

import "fmt"

// SampleID identifies a specimen. It is immutable once cataloged.
type SampleID string

// BatchID identifies an append-only batch. A sample may have many batches.
type BatchID string

// Revision identifies a specific immutable batch revision.
type Revision string

// TubeID is the globally unique identity of a mother or child tube.
type TubeID string

// SessionID identifies an aliquot session. It is declared here because both
// the catalog (containers) and the aliquot session reference it.
type SessionID string

// Specimen is an immutable sample fact.
type Specimen struct {
	ID          SampleID
	Description string
}

// BatchRevision is an append-only batch fact keyed by
// (sample_id, batch_id, revision).
type BatchRevision struct {
	SampleID SampleID
	BatchID  BatchID
	Revision Revision
}

// ContainerType distinguishes a mother tube from a child tube.
type ContainerType string

const (
	MotherContainer ContainerType = "mother"
	ChildContainer  ContainerType = "child"
)

// Container is a tube record. The parent/child relationship is not inferred
// from this table; it is maintained separately by the lineage graph.
type Container struct {
	ID          TubeID
	Type        ContainerType
	SampleID    SampleID
	BatchID     BatchID
	Revision    Revision
	RemainingUL int64
	Status      string
	SessionID   SessionID // empty when the tube is not bound to a session
}

// Container status values persisted on container rows. The aliquot state
// machine interprets the same strings through aliquot.MotherStatus.
const (
	ContainerStatusAvailable   = "available"
	ContainerStatusReserved    = "reserved"
	ContainerStatusThawed      = "thawed"
	ContainerStatusAliquoting  = "aliquoting"
	ContainerStatusDepleted    = "depleted"
	ContainerStatusFinalized   = "finalized"
	ContainerStatusQuarantined = "quarantined"
)

// IsMother reports whether the container is a mother tube.
func (c Container) IsMother() bool { return c.Type == MotherContainer }

// IsChild reports whether the container is a child tube.
func (c Container) IsChild() bool { return c.Type == ChildContainer }

// IsAvailable reports whether a mother container is free to be reserved.
func (c Container) IsAvailable() bool {
	return c.IsMother() && c.Status == ContainerStatusAvailable
}

// Matches reports whether the container references the given specimen and
// batch revision.
func (c Container) Matches(s Specimen, b BatchRevision) bool {
	return c.SampleID == s.ID &&
		c.BatchID == b.BatchID &&
		c.Revision == b.Revision
}

// ValidateReference verifies that a specimen, batch revision, and container
// are mutually consistent. Any mismatch is a stable business rejection that
// must occur before the first write (failure boundary 1).
func ValidateReference(s Specimen, b BatchRevision, c Container) error {
	if c.SampleID != s.ID {
		return fmt.Errorf("container %q sample %q does not match specimen %q", c.ID, c.SampleID, s.ID)
	}
	if b.SampleID != s.ID {
		return fmt.Errorf("batch revision sample %q does not match specimen %q", b.SampleID, s.ID)
	}
	if c.BatchID != b.BatchID || c.Revision != b.Revision {
		return fmt.Errorf("container %q batch %q/%q does not match %q/%q",
			c.ID, c.BatchID, c.Revision, b.BatchID, b.Revision)
	}
	return nil
}
