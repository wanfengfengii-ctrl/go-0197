package aggregate

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/volume"
)

// service is the concrete aggregate implementation. It loads catalog, session,
// ledger, and lineage state, executes commands with operation-idempotency
// semantics, and serializes concurrent mutations with per-key in-process locks.
type service struct {
	store store.Store
	hooks *store.Hooks
	locks sync.Map // key string -> *sync.Mutex
}

// New constructs the domain Service backed by a store and optional test hooks.
func New(s store.Store, hooks *store.Hooks) Service {
	return &service{store: s, hooks: hooks}
}

func (s *service) barrier(name string) {
	if s.hooks != nil && s.hooks.Barrier != nil {
		s.hooks.Barrier(name)
	}
}

func (s *service) lockKey(k string) *sync.Mutex {
	v, _ := s.locks.LoadOrStore(k, &sync.Mutex{})
	return v.(*sync.Mutex)
}

func newSessionID() catalog.SessionID {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return catalog.SessionID("s_" + hex.EncodeToString(b))
}

// checkOperation consults the persisted operation result. It returns the stored
// result and a found flag; a fingerprint mismatch produces OPERATION_CONFLICT.
func (s *service) checkOperation(ctx context.Context, opID, fp string) (*store.OperationResult, bool, error) {
	res, err := s.store.LoadOperation(ctx, opID)
	if err != nil {
		return nil, false, err
	}
	if res == nil {
		return nil, false, nil
	}
	if res.Fingerprint != fp {
		return res, true, aliquot.NewError(aliquot.CodeOperationConflict, res.Revision, "operation id reused with different content")
	}
	return res, true, nil
}

// replayResult reproduces the persisted outcome of an already-consumed operation
// ID, returning the original revision and terminal/error without re-executing.
func (s *service) replayResult(ctx context.Context, sessionID catalog.SessionID, res *store.OperationResult) (SessionView, error) {
	if res.Code != "ok" {
		return SessionView{}, aliquot.NewError(aliquot.Code(res.Code), res.Revision, res.Message)
	}
	sess, err := s.store.LoadSession(ctx, sessionID)
	if err != nil {
		return SessionView{}, err
	}
	if sess == nil {
		return SessionView{}, aliquot.NewError(aliquot.CodeInvalidState, res.Revision, "session not found")
	}
	view := buildView(sess)
	view.RevisionNum = res.Revision
	return view, nil
}

// reject persists a deterministic business rejection and returns it, so a retry
// of the same operation ID yields the same stable rejection (domain rule 12).
func (s *service) reject(ctx context.Context, opID, fp string, e *aliquot.Error) (SessionView, error) {
	if opID != "" {
		_ = s.store.RecordRejection(ctx, opID, fp, e)
	}
	return SessionView{}, e
}

func (s *service) loadSession(ctx context.Context, id catalog.SessionID) (*store.Session, error) {
	sess, err := s.store.LoadSession(ctx, id)
	if err != nil {
		return nil, err
	}
	if sess == nil {
		return nil, aliquot.NewError(aliquot.CodeInvalidState, 0, "session not found")
	}
	return sess, nil
}

// loadSessionForCommand loads a session and rejects commands targeting a
// terminal session (the stable conflict returned to a terminal-race loser).
func (s *service) loadSessionForCommand(ctx context.Context, id catalog.SessionID) (*store.Session, error) {
	sess, err := s.loadSession(ctx, id)
	if err != nil {
		return nil, err
	}
	if sess.TerminalKind != "" {
		return nil, aliquot.NewError(aliquot.CodeTerminalConflict, sess.RevisionNum, "session already terminal")
	}
	return sess, nil
}

func (s *service) viewAfter(ctx context.Context, id catalog.SessionID) (SessionView, error) {
	sess, err := s.loadSession(ctx, id)
	if err != nil {
		return SessionView{}, err
	}
	return buildView(sess), nil
}

// ReserveMother reserves a mother tube and freezes the child plan (acceptance 1).
func (s *service) ReserveMother(ctx context.Context, c ReserveMotherCommand) (SessionView, error) {
	s.barrier("reserve:" + string(c.MotherTubeID))
	mu := s.lockKey("mother:" + string(c.MotherTubeID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintReserve(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, res.SessionID, res)
	}

	mother, err := s.store.LoadMother(ctx, c.MotherTubeID)
	if err != nil {
		return SessionView{}, err
	}
	if mother == nil {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, 0, "mother not found"))
	}

	switch {
	case mother.Status == catalog.ContainerStatusAvailable:
		// eligible
	case mother.Status == catalog.ContainerStatusFinalized || mother.Status == catalog.ContainerStatusQuarantined:
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, 0, "mother is terminal"))
	default:
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeMotherAlreadyReserved, 0, "mother already reserved"))
	}

	if c.SampleID != mother.SampleID {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeSampleMismatch, 0, "sample mismatch"))
	}
	if c.BatchID != mother.BatchID || c.Revision != mother.Revision {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeBatchMismatch, 0, "batch revision mismatch"))
	}
	if c.LockedVolume != mother.RemainingUL {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeOverAllocation, 0, "locked volume must equal mother remaining volume"))
	}
	if err := aliquot.ValidatePlan(c.LockedVolume, c.Children); err != nil {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeOverAllocation, 0, err.Error()))
	}

	childIDs := make([]catalog.TubeID, 0, len(c.Children))
	for _, ch := range c.Children {
		childIDs = append(childIDs, ch.ChildTubeID)
	}
	claimed, err := s.store.ClaimedTubeNumbers(ctx, childIDs)
	if err != nil {
		return SessionView{}, err
	}
	if err := catalog.EnsureTubeNumbersAvailable(childIDs, claimed); err != nil {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeChildNumberConflict, 0, err.Error()))
	}

	sessionID := newSessionID()
	result, err := s.store.Reserve(ctx, store.ReserveRequest{
		OperationID:    c.OperationID,
		Fingerprint:    fp,
		SessionID:      sessionID,
		MotherTubeID:   c.MotherTubeID,
		SampleID:       c.SampleID,
		BatchID:        c.BatchID,
		Revision:       c.Revision,
		LockedVolumeUL: c.LockedVolume,
		Children:       aliquot.SortPlan(c.Children),
	})
	if err != nil {
		return SessionView{}, err
	}
	_ = result
	return s.viewAfter(ctx, sessionID)
}

// ConfirmThaw confirms the thaw evidence, or quarantines the batch on mismatch.
func (s *service) ConfirmThaw(ctx context.Context, c ConfirmThawCommand) (SessionView, error) {
	s.barrier("thaw:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintThaw(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}
	if sess.Status != aliquot.StatusReserved {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "session not reserved"))
	}

	evidence := aliquot.ThawEvidence{
		MotherTubeID: c.MotherTubeID,
		SampleID:     c.SampleID,
		BatchID:      c.BatchID,
		Revision:     c.Revision,
	}
	snap := aliquot.Snapshot{
		SessionID:      sess.ID,
		MotherTubeID:   sess.MotherTubeID,
		SampleID:       sess.SampleID,
		BatchID:        sess.BatchID,
		Revision:       sess.Revision,
		LockedVolumeUL: sess.LockedVolumeUL,
	}
	if !evidence.Matches(snap) {
		return s.quarantineFor(ctx, c.SessionID, c.OperationID, fp, sess.RevisionNum, "thaw evidence mismatch")
	}

	if _, err := s.store.Thaw(ctx, store.ThawRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
		MotherTubeID:   c.MotherTubeID,
		SampleID:       c.SampleID,
		BatchID:        c.BatchID,
		Revision:       c.Revision,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// CreateChild creates exactly one planned child at its frozen allocation.
func (s *service) CreateChild(ctx context.Context, c CreateChildCommand) (SessionView, error) {
	s.barrier("child:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintCreateChild(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}
	if sess.Status != aliquot.StatusThawed && sess.Status != aliquot.StatusAliquoting {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "cannot create child in current state"))
	}

	var planned *store.PlannedChild
	for i := range sess.Children {
		if sess.Children[i].ChildTubeID == c.ChildTubeID {
			planned = &sess.Children[i]
			break
		}
	}
	if planned == nil {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "child not in plan"))
	}
	if planned.Created {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "child already created"))
	}

	ledger := volume.NewLedger(sess.LockedVolumeUL, sess.Entries)
	remaining := ledger.Remaining()
	if planned.PlannedVolumeUL > remaining {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeOverAllocation, sess.RevisionNum, "allocation exceeds remaining volume"))
	}

	newStatus := aliquot.StatusAliquoting
	if remaining-planned.PlannedVolumeUL == 0 {
		newStatus = aliquot.StatusDepleted
	}

	if _, err := s.store.CreateChild(ctx, store.ChildRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
		ChildTubeID:    c.ChildTubeID,
		NewStatus:      newStatus,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// RecordLoss records a non-negative integer loss without consuming outstanding
// planned volume (acceptance 3, domain rule 8).
func (s *service) RecordLoss(ctx context.Context, c RecordLossCommand) (SessionView, error) {
	s.barrier("loss:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintRecordLoss(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}
	if sess.Status != aliquot.StatusThawed && sess.Status != aliquot.StatusAliquoting {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "cannot record loss in current state"))
	}
	if c.LossUL < 0 {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "loss must be non-negative"))
	}

	ledger := volume.NewLedger(sess.LockedVolumeUL, sess.Entries)
	remaining := ledger.Remaining()
	outstanding := outstandingPlan(sess.Children)
	if c.LossUL > remaining-outstanding {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeLossExceedsOutstanding, sess.RevisionNum, "loss would consume outstanding plan"))
	}

	newStatus := sess.Status
	if remaining-c.LossUL == 0 {
		newStatus = aliquot.StatusDepleted
	}

	if _, err := s.store.RecordLoss(ctx, store.LossRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
		LossUL:         c.LossUL,
		NewStatus:      newStatus,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// VerifyRefreeze records a per-child refreeze verification, or quarantines the
// batch on evidence mismatch (business flow 5).
func (s *service) VerifyRefreeze(ctx context.Context, c VerifyRefreezeCommand) (SessionView, error) {
	s.barrier("verify:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintVerify(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}
	if sess.Status != aliquot.StatusAliquoting && sess.Status != aliquot.StatusDepleted {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "cannot verify in current state"))
	}

	var planned *store.PlannedChild
	for i := range sess.Children {
		if sess.Children[i].ChildTubeID == c.ChildTubeID {
			planned = &sess.Children[i]
			break
		}
	}
	if planned == nil {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "child not in plan"))
	}
	if !planned.Created {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "child not created"))
	}
	if _, ok := sess.Verifications[c.ChildTubeID]; ok {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "child already verified"))
	}

	if c.SampleID != sess.SampleID || c.BatchID != sess.BatchID || c.Revision != sess.Revision || c.AllocationUL != planned.PlannedVolumeUL {
		return s.quarantineFor(ctx, c.SessionID, c.OperationID, fp, sess.RevisionNum, "refreeze evidence mismatch")
	}

	if _, err := s.store.VerifyRefreeze(ctx, store.VerifyRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
		ChildTubeID:    c.ChildTubeID,
		SampleID:       c.SampleID,
		BatchID:        c.BatchID,
		Revision:       c.Revision,
		AllocationUL:   c.AllocationUL,
		FreezeRunID:    c.FreezeRunID,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// FinalizeLineage produces the unique immutable lineage manifest (acceptance 6).
func (s *service) FinalizeLineage(ctx context.Context, c FinalizeLineageCommand) (SessionView, error) {
	s.barrier("finalize:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintFinalize(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}
	if sess.Status != aliquot.StatusDepleted {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "mother not depleted"))
	}

	missing := lineage.MissingVerifications(plannedVolumeIDs(sess.Children), sess.Verifications)
	if len(missing) > 0 {
		return s.reject(ctx, c.OperationID, fp, aliquot.NewError(aliquot.CodeMissingVerification, sess.RevisionNum, fmt.Sprintf("missing verifications: %v", missing)))
	}

	if _, err := s.store.Finalize(ctx, store.FinalizeRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// QuarantineBatch quarantines the whole session and its affected tubes.
func (s *service) QuarantineBatch(ctx context.Context, c QuarantineBatchCommand) (SessionView, error) {
	s.barrier("quarantine:" + string(c.SessionID))
	mu := s.lockKey("session:" + string(c.SessionID))
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprintQuarantine(c)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return SessionView{}, err
	}
	if found {
		return s.replayResult(ctx, c.SessionID, res)
	}

	sess, err := s.loadSessionForCommand(ctx, c.SessionID)
	if err != nil {
		return s.reject(ctx, c.OperationID, fp, errToAliquot(err))
	}

	if _, err := s.store.Quarantine(ctx, store.QuarantineRequest{
		CommandRequest: store.CommandRequest{OperationID: c.OperationID, Fingerprint: fp, SessionID: c.SessionID, ExpectedRevision: sess.RevisionNum},
		Reason:         c.Reason,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, c.SessionID)
}

// quarantineFor performs an atomic batch quarantine triggered by an evidence
// mismatch inside the thaw or refreeze command (domain rule 7).
func (s *service) quarantineFor(ctx context.Context, sessionID catalog.SessionID, opID, fp string, expectedRev int64, reason string) (SessionView, error) {
	if _, err := s.store.Quarantine(ctx, store.QuarantineRequest{
		CommandRequest: store.CommandRequest{OperationID: opID, Fingerprint: fp, SessionID: sessionID, ExpectedRevision: expectedRev},
		Reason:         reason,
	}); err != nil {
		return SessionView{}, err
	}
	return s.viewAfter(ctx, sessionID)
}

// GetSession returns the consistent read model for a session.
func (s *service) GetSession(ctx context.Context, id catalog.SessionID) (SessionView, error) {
	sess, err := s.loadSession(ctx, id)
	if err != nil {
		return SessionView{}, err
	}
	return buildView(sess), nil
}

// errToAliquot converts a terminal-conflict error (or passes through other
// *aliquot.Error values) so it can be persisted as a stable rejection.
func errToAliquot(err error) *aliquot.Error {
	if e, ok := err.(*aliquot.Error); ok {
		return e
	}
	return aliquot.NewError(aliquot.CodeInvalidState, 0, err.Error())
}

// BootstrapCatalog registers catalog facts in an empty database, idempotently
// keyed by operation ID (public interface 2).
func (s *service) BootstrapCatalog(ctx context.Context, c BootstrapCommand) error {
	mu := s.lockKey("bootstrap")
	mu.Lock()
	defer mu.Unlock()

	fp := fingerprint("bootstrap", c.Facts)
	res, found, err := s.checkOperation(ctx, c.OperationID, fp)
	if err != nil {
		return err
	}
	if found {
		if res.Code == "ok" {
			return nil
		}
		return aliquot.NewError(aliquot.Code(res.Code), res.Revision, res.Message)
	}
	if err := s.store.Bootstrap(ctx, c.OperationID, fp, c.Facts); err != nil {
		if err == store.ErrAlreadyBootstrapped {
			return aliquot.NewError(aliquot.CodeInvalidState, 0, "catalog already bootstrapped")
		}
		return err
	}
	return nil
}

// GetManifest returns the immutable lineage manifest for a finalized session.
func (s *service) GetManifest(ctx context.Context, id catalog.SessionID) (*lineage.Manifest, []lineage.ManifestItem, error) {
	sess, err := s.loadSession(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if sess.Manifest == nil {
		return nil, nil, aliquot.NewError(aliquot.CodeInvalidState, sess.RevisionNum, "no manifest for session")
	}
	return sess.Manifest, sess.ManifestItems, nil
}
