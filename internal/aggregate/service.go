// Package aggregate assembles catalog, session, ledger, and lineage state,
// executes commands, applies operation-idempotency semantics, and produces a
// consistent session view. It exposes the domain Service interface that public
// tests and the HTTP adapter both consume (public interface 1).
package aggregate

import (
	"context"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
)

// Service is the domain entry point. Public tests can bypass the network and
// call the same aggregate logic used by the HTTP adapter.
type Service interface {
	BootstrapCatalog(ctx context.Context, c BootstrapCommand) error
	ReserveMother(ctx context.Context, c ReserveMotherCommand) (SessionView, error)
	ConfirmThaw(ctx context.Context, c ConfirmThawCommand) (SessionView, error)
	CreateChild(ctx context.Context, c CreateChildCommand) (SessionView, error)
	RecordLoss(ctx context.Context, c RecordLossCommand) (SessionView, error)
	VerifyRefreeze(ctx context.Context, c VerifyRefreezeCommand) (SessionView, error)
	FinalizeLineage(ctx context.Context, c FinalizeLineageCommand) (SessionView, error)
	QuarantineBatch(ctx context.Context, c QuarantineBatchCommand) (SessionView, error)
	GetSession(ctx context.Context, id catalog.SessionID) (SessionView, error)
	GetManifest(ctx context.Context, id catalog.SessionID) (*lineage.Manifest, []lineage.ManifestItem, error)
}

// BootstrapCommand registers catalog facts in an empty database.
type BootstrapCommand struct {
	OperationID string
	Facts       store.Facts
}

// Operation carries the fields shared by every mutation command (domain rule 11).
type Operation struct {
	OperationID      string
	ExpectedRevision int64
}

// ReserveMotherCommand reserves a mother tube and freezes the child plan.
type ReserveMotherCommand struct {
	Operation
	MotherTubeID catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
	LockedVolume int64
	Children     []aliquot.ChildPlan
}

// ConfirmThawCommand reports the physically scanned thaw evidence.
type ConfirmThawCommand struct {
	Operation
	SessionID    catalog.SessionID
	MotherTubeID catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
}

// CreateChildCommand creates one planned child at its exact frozen allocation.
type CreateChildCommand struct {
	Operation
	SessionID   catalog.SessionID
	ChildTubeID catalog.TubeID
}

// RecordLossCommand records a non-negative integer loss in microliters.
type RecordLossCommand struct {
	Operation
	SessionID catalog.SessionID
	LossUL    int64
}

// VerifyRefreezeCommand records a per-child refreeze verification.
type VerifyRefreezeCommand struct {
	Operation
	SessionID    catalog.SessionID
	ChildTubeID  catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
	AllocationUL int64
	FreezeRunID  string
}

// FinalizeLineageCommand produces the unique immutable lineage manifest.
type FinalizeLineageCommand struct {
	Operation
	SessionID catalog.SessionID
}

// QuarantineBatchCommand quarantines the whole session and its affected tubes.
type QuarantineBatchCommand struct {
	Operation
	SessionID catalog.SessionID
	Reason    string
}

// TerminalKind is the kind of a committed terminal outcome.
type TerminalKind string

const (
	TerminalFinalized   TerminalKind = "finalized"
	TerminalQuarantined TerminalKind = "quarantined"
)

// SessionView is the consistent read model returned by GetSession and every
// mutation (public interface 6).
type SessionView struct {
	SessionID      catalog.SessionID    `json:"session_id"`
	MotherTubeID   catalog.TubeID       `json:"mother_tube_id"`
	SampleID       catalog.SampleID     `json:"sample_id"`
	BatchID        catalog.BatchID      `json:"batch_id"`
	Revision       catalog.Revision     `json:"revision"`
	Status         aliquot.MotherStatus `json:"status"`
	RevisionNum    int64                `json:"revision_num"`
	LockedVolumeUL int64                `json:"locked_volume_uL"`
	AllocatedUL    int64                `json:"allocated_uL"`
	LossUL         int64                `json:"loss_uL"`
	RemainingUL    int64                `json:"remaining_uL"`
	Children       []ChildView          `json:"children"`
	MissingVerify  []catalog.TubeID     `json:"missing_verify"`
	Terminal       *TerminalView        `json:"terminal,omitempty"`
}

// ChildView is a per-child status line ordered by plan ordinal.
type ChildView struct {
	Ordinal         int            `json:"ordinal"`
	ChildTubeID     catalog.TubeID `json:"child_tube_id"`
	PlannedVolumeUL int64          `json:"planned_volume_uL"`
	Created         bool           `json:"created"`
	Verified        bool           `json:"verified"`
}

// TerminalView is the committed terminal outcome, if any.
type TerminalView struct {
	Kind     TerminalKind `json:"kind"`
	Reason   string       `json:"reason"`
	Revision int64        `json:"revision"`
}
