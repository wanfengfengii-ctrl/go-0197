package aggregate

import (
	"github.com/aliquotseal/maternal-lineage-freeze/internal/catalog"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/lineage"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/store"
	"github.com/aliquotseal/maternal-lineage-freeze/internal/volume"
)

// buildView projects a fully loaded store session into the consistent read model
// returned by GetSession and every mutation (public interface 6).
func buildView(sess *store.Session) SessionView {
	ledger := volume.NewLedger(sess.LockedVolumeUL, sess.Entries)

	view := SessionView{
		SessionID:      sess.ID,
		MotherTubeID:   sess.MotherTubeID,
		SampleID:       sess.SampleID,
		BatchID:        sess.BatchID,
		Revision:       sess.Revision,
		Status:         sess.Status,
		RevisionNum:    sess.RevisionNum,
		LockedVolumeUL: sess.LockedVolumeUL,
		AllocatedUL:    ledger.Allocated(),
		LossUL:         ledger.Loss(),
		RemainingUL:    ledger.Remaining(),
	}

	plannedIDs := make([]catalog.TubeID, 0, len(sess.Children))
	for _, c := range sess.Children {
		view.Children = append(view.Children, ChildView{
			Ordinal:         c.Ordinal,
			ChildTubeID:     c.ChildTubeID,
			PlannedVolumeUL: c.PlannedVolumeUL,
			Created:         c.Created,
			Verified:        sess.Verifications[c.ChildTubeID].ChildTubeID != "",
		})
		plannedIDs = append(plannedIDs, c.ChildTubeID)
	}

	view.MissingVerify = lineage.MissingVerifications(plannedIDs, sess.Verifications)

	if sess.TerminalKind != "" {
		view.Terminal = &TerminalView{
			Kind:     TerminalKind(sess.TerminalKind),
			Reason:   sess.TerminalReason,
			Revision: sess.RevisionNum,
		}
	}
	return view
}

// plannedVolumeIDs returns the planned child tube IDs in ordinal order.
func plannedVolumeIDs(children []store.PlannedChild) []catalog.TubeID {
	ids := make([]catalog.TubeID, 0, len(children))
	for _, c := range children {
		ids = append(ids, c.ChildTubeID)
	}
	return ids
}

// outstandingPlan computes the total planned volume of not-yet-created children.
func outstandingPlan(children []store.PlannedChild) int64 {
	var total int64
	for _, c := range children {
		if !c.Created {
			total += c.PlannedVolumeUL
		}
	}
	return total
}
