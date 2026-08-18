// Package aliquot defines the aliquot session model: the mother tube state
// machine, the immutable reservation snapshot, the ordered child plan, thaw
// evidence, loss records, and terminal commands. It centralizes the stable
// domain error codes (domain rule 1 and the component contract).
package aliquot

import (
	"fmt"
)

// MotherStatus is the state of the mother tube across its irreversible
// aliquoting lifecycle.
type MotherStatus string

const (
	StatusAvailable   MotherStatus = "available"
	StatusReserved    MotherStatus = "reserved"
	StatusThawed      MotherStatus = "thawed"
	StatusAliquoting  MotherStatus = "aliquoting"
	StatusDepleted    MotherStatus = "depleted"
	StatusFinalized   MotherStatus = "finalized"
	StatusQuarantined MotherStatus = "quarantined"
)

// transitions encodes the allowed mother state machine (domain rule 1):
//
//	available -> reserved -> thawed -> aliquoting -> depleted -> finalized;
//	reserved/thawed/aliquoting/depleted -> quarantined.
var transitions = map[MotherStatus]map[MotherStatus]bool{
	StatusAvailable:   {StatusReserved: true},
	StatusReserved:    {StatusThawed: true, StatusQuarantined: true},
	StatusThawed:      {StatusAliquoting: true, StatusQuarantined: true},
	StatusAliquoting:  {StatusDepleted: true, StatusQuarantined: true},
	StatusDepleted:    {StatusFinalized: true, StatusQuarantined: true},
	StatusFinalized:   {},
	StatusQuarantined: {},
}

// IsTerminal reports whether the status is a final, irreversible outcome.
func (s MotherStatus) IsTerminal() bool {
	return s == StatusFinalized || s == StatusQuarantined
}

// CanTransition reports whether the state machine permits moving from -> to.
func CanTransition(from, to MotherStatus) bool {
	return transitions[from][to]
}

// Code is a stable machine-readable domain error code.
type Code string

const (
	CodeMotherAlreadyReserved  Code = "MOTHER_ALREADY_RESERVED"
	CodeSampleMismatch         Code = "SAMPLE_MISMATCH"
	CodeBatchMismatch          Code = "BATCH_MISMATCH"
	CodeChildNumberConflict    Code = "CHILD_NUMBER_CONFLICT"
	CodeOverAllocation         Code = "OVER_ALLOCATION"
	CodeLossExceedsOutstanding Code = "LOSS_EXCEEDS_OUTSTANDING"
	CodeMissingVerification    Code = "MISSING_VERIFICATION"
	CodeOperationConflict      Code = "OPERATION_CONFLICT"
	CodeRevisionConflict       Code = "REVISION_CONFLICT"
	CodeTerminalConflict       Code = "TERMINAL_CONFLICT"
	CodeInvalidState           Code = "INVALID_STATE"
)

// Error is a stable domain error carrying a machine code and the current
// session revision at the time of rejection.
type Error struct {
	Code     Code
	Revision int64
	Message  string
}

func (e *Error) Error() string {
	if e.Message == "" {
		return string(e.Code)
	}
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewError constructs a stable domain error with an optional revision.
func NewError(code Code, revision int64, msg string) *Error {
	return &Error{Code: code, Revision: revision, Message: msg}
}
