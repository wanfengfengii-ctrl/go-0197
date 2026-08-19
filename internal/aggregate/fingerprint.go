package aggregate

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
)

// fingerprint hashes a command kind plus its semantic fields into a stable
// identifier (domain rule 11). The operation ID is deliberately excluded; only
// the command type, target, and semantic content participate, so a reused
// operation ID with different content yields OPERATION_CONFLICT.
func fingerprint(kind string, v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return kind + ":unstable"
	}
	sum := sha256.Sum256(append([]byte(kind+":"), b...))
	return hex.EncodeToString(sum[:])
}

// canonicalChildPlan returns a copy of the plan ordered by (ordinal, child tube
// number). Reservation freezes the plan in ordinal order (domain rule 2: the
// plan order is the ordinal, carried by each child), so the array order a
// caller supplies is presentation only. Two requests carrying the same children
// in a different array order are semantically identical and must share a
// fingerprint so the retry replays the original result instead of conflicting
// (domain rule 11). Every per-child ordinal, tube number, and allocation still
// participates in the marshaled value, so any genuine content change yields a
// distinct fingerprint.
func canonicalChildPlan(plan []aliquot.ChildPlan) []aliquot.ChildPlan {
	out := make([]aliquot.ChildPlan, len(plan))
	copy(out, plan)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Ordinal != out[j].Ordinal {
			return out[i].Ordinal < out[j].Ordinal
		}
		return out[i].ChildTubeID < out[j].ChildTubeID
	})
	return out
}

func fingerprintReserve(c ReserveMotherCommand) string {
	return fingerprint("reserve", struct {
		Mother   catalog.TubeID
		Sample   catalog.SampleID
		Batch    catalog.BatchID
		Revision catalog.Revision
		Locked   int64
		Children []aliquot.ChildPlan
		Expected int64
	}{c.MotherTubeID, c.SampleID, c.BatchID, c.Revision, c.LockedVolume, canonicalChildPlan(c.Children), c.ExpectedRevision})
}

func fingerprintThaw(c ConfirmThawCommand) string {
	return fingerprint("thaw", struct {
		Session catalog.SessionID
		Mother  catalog.TubeID
		Sample  catalog.SampleID
		Batch   catalog.BatchID
		Rev     catalog.Revision
		Exp     int64
	}{c.SessionID, c.MotherTubeID, c.SampleID, c.BatchID, c.Revision, c.ExpectedRevision})
}

func fingerprintCreateChild(c CreateChildCommand) string {
	return fingerprint("create_child", struct {
		Session catalog.SessionID
		Child   catalog.TubeID
		Exp     int64
	}{c.SessionID, c.ChildTubeID, c.ExpectedRevision})
}

func fingerprintRecordLoss(c RecordLossCommand) string {
	return fingerprint("record_loss", struct {
		Session catalog.SessionID
		Loss    int64
		Exp     int64
	}{c.SessionID, c.LossUL, c.ExpectedRevision})
}

func fingerprintVerify(c VerifyRefreezeCommand) string {
	return fingerprint("verify_refreeze", struct {
		Session catalog.SessionID
		Child   catalog.TubeID
		Sample  catalog.SampleID
		Batch   catalog.BatchID
		Rev     catalog.Revision
		Alloc   int64
		Freeze  string
		Exp     int64
	}{c.SessionID, c.ChildTubeID, c.SampleID, c.BatchID, c.Revision, c.AllocationUL, c.FreezeRunID, c.ExpectedRevision})
}

func fingerprintFinalize(c FinalizeLineageCommand) string {
	return fingerprint("finalize", struct {
		Session catalog.SessionID
		Exp     int64
	}{c.SessionID, c.ExpectedRevision})
}

func fingerprintQuarantine(c QuarantineBatchCommand) string {
	return fingerprint("quarantine", struct {
		Session catalog.SessionID
		Reason  string
		Exp     int64
	}{c.SessionID, c.Reason, c.ExpectedRevision})
}
