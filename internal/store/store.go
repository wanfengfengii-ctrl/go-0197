// Package store defines the persistence boundary for the aliquot service: the
// repository interface, the deterministic test hooks (sync barriers and named
// transaction checkpoints), and the shared load/command types. The concrete
// SQLite implementation lives in internal/store/sqlite.
package store

import (
	"context"
	"errors"
	"time"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/volume"
)

// Checkpoint names the named transaction interrupt points required by failure
// boundaries 2 and 3. Returning an error from a checkpoint aborts and rolls back
// the enclosing transaction.
type Checkpoint string

const (
	CheckpointAfterChildInsert     Checkpoint = "after_child_insert"
	CheckpointAfterVolumeDebit     Checkpoint = "after_volume_debit"
	CheckpointAfterLineageInsert   Checkpoint = "after_lineage_insert"
	CheckpointBeforeCommit         Checkpoint = "before_commit"
	CheckpointAfterTerminalClaim   Checkpoint = "after_terminal_claim"
	CheckpointAfterManifestInsert  Checkpoint = "after_manifest_insert"
	CheckpointBeforeTerminalCommit Checkpoint = "before_terminal_commit"
)

// Hooks carries the deterministic concurrency and recovery controls used only
// by tests. The production service passes nil hooks, which are no-ops.
type Hooks struct {
	// Interrupt, when non-nil, is consulted at every named checkpoint. A
	// non-nil return aborts the current transaction (transaction interrupt).
	Interrupt func(Checkpoint) error
	// Barrier, when non-nil, blocks the caller at a named synchronization point
	// until the test releases it, enabling deterministic concurrent ordering.
	Barrier func(name string)
}

// check consults the interrupt hook for a named checkpoint.
func (h *Hooks) check(c Checkpoint) error {
	if h != nil && h.Interrupt != nil {
		return h.Interrupt(c)
	}
	return nil
}

// barrier blocks at a named synchronization point when a hook is configured.
func (h *Hooks) barrier(name string) {
	if h != nil && h.Barrier != nil {
		h.Barrier(name)
	}
}

// TerminalKind is the persisted terminal outcome kind.
type TerminalKind string

const (
	TerminalFinalized   TerminalKind = "finalized"
	TerminalQuarantined TerminalKind = "quarantined"
)

// ErrAlreadyBootstrapped signals that the catalog already holds facts.
var ErrAlreadyBootstrapped = errors.New("catalog already bootstrapped")

// Facts is the set of catalog facts registered by a bootstrap operation.
type Facts struct {
	Specimens      []catalog.Specimen
	BatchRevisions []catalog.BatchRevision
	Mothers        []MotherFact
}

// MotherFact is a single available mother tube registered at bootstrap.
type MotherFact struct {
	TubeID   catalog.TubeID
	SampleID catalog.SampleID
	BatchID  catalog.BatchID
	Revision catalog.Revision
	VolumeUL int64
}

// PlannedChild is a loaded planned-child row.
type PlannedChild struct {
	Ordinal         int
	ChildTubeID     catalog.TubeID
	PlannedVolumeUL int64
	Created         bool
}

// Session is the fully loaded state of an aliquot session, assembled from the
// catalog, session, ledger, lineage, and verification tables.
type Session struct {
	ID             catalog.SessionID
	MotherTubeID   catalog.TubeID
	SampleID       catalog.SampleID
	BatchID        catalog.BatchID
	Revision       catalog.Revision
	LockedVolumeUL int64
	Status         aliquot.MotherStatus
	RevisionNum    int64
	TerminalKind   TerminalKind
	TerminalReason string
	CreatedAt      time.Time

	Children      []PlannedChild
	Entries       []volume.Entry
	Edges         []lineage.Edge
	Verifications map[catalog.TubeID]lineage.Verification
	Manifest      *lineage.Manifest
	ManifestItems []lineage.ManifestItem
}

// OperationResult is the persisted outcome of a consumed operation ID.
type OperationResult struct {
	OperationID string
	Fingerprint string
	Code        string // "" on success; otherwise the stable machine code
	Message     string
	Revision    int64
	Terminal    TerminalKind
	SessionID   catalog.SessionID
}

// CommandResult is the committed outcome of a successful write.
type CommandResult struct {
	Revision int64
	Terminal TerminalKind
}

// CommandRequest carries the fields shared by every session-scoped mutation.
type CommandRequest struct {
	OperationID      string
	Fingerprint      string
	SessionID        catalog.SessionID
	ExpectedRevision int64
}

// ReserveRequest persists a new reservation session.
type ReserveRequest struct {
	OperationID    string
	Fingerprint    string
	SessionID      catalog.SessionID
	MotherTubeID   catalog.TubeID
	SampleID       catalog.SampleID
	BatchID        catalog.BatchID
	Revision       catalog.Revision
	LockedVolumeUL int64
	Children       []aliquot.ChildPlan
}

// ThawRequest persists a confirmed thaw transition.
type ThawRequest struct {
	CommandRequest
	MotherTubeID catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
}

// ChildRequest persists the creation of one planned child.
type ChildRequest struct {
	CommandRequest
	ChildTubeID catalog.TubeID
	NewStatus   aliquot.MotherStatus
}

// LossRequest persists a recorded non-negative integer loss.
type LossRequest struct {
	CommandRequest
	LossUL    int64
	NewStatus aliquot.MotherStatus
}

// VerifyRequest persists a per-child refreeze verification.
type VerifyRequest struct {
	CommandRequest
	ChildTubeID  catalog.TubeID
	SampleID     catalog.SampleID
	BatchID      catalog.BatchID
	Revision     catalog.Revision
	AllocationUL int64
	FreezeRunID  string
}

// FinalizeRequest persists a finalize terminal transition.
type FinalizeRequest struct {
	CommandRequest
}

// QuarantineRequest persists a quarantine terminal transition.
type QuarantineRequest struct {
	CommandRequest
	Reason string
}

// Store is the repository interface consumed by the aggregate service. It
// exposes transactional command methods plus recovery verification (public
// interface 8 and failure boundary 6).
type Store interface {
	Bootstrap(ctx context.Context, opID, fingerprint string, facts Facts) error
	CatalogEmpty(ctx context.Context) (bool, error)

	LoadMother(ctx context.Context, id catalog.TubeID) (*catalog.Container, error)
	LoadSession(ctx context.Context, id catalog.SessionID) (*Session, error)
	LoadOperation(ctx context.Context, opID string) (*OperationResult, error)
	ClaimedTubeNumbers(ctx context.Context, ids []catalog.TubeID) (catalog.TubeSet, error)

	Reserve(ctx context.Context, r ReserveRequest) (CommandResult, error)
	Thaw(ctx context.Context, r ThawRequest) (CommandResult, error)
	CreateChild(ctx context.Context, r ChildRequest) (CommandResult, error)
	RecordLoss(ctx context.Context, r LossRequest) (CommandResult, error)
	VerifyRefreeze(ctx context.Context, r VerifyRequest) (CommandResult, error)
	Finalize(ctx context.Context, r FinalizeRequest) (CommandResult, error)
	Quarantine(ctx context.Context, r QuarantineRequest) (CommandResult, error)

	RecordRejection(ctx context.Context, opID, fingerprint string, e *aliquot.Error) error
	Recover(ctx context.Context) error
	Close() error
}
