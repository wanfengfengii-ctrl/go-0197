package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/aliquotseal/maternal-lineage-freeze/internal/aliquot"
)

// writeJSON writes a JSON response with the given status.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorBody is the stable error envelope returned by every non-2xx response.
type errorBody struct {
	Error errorDetail `json:"error"`
}

type errorDetail struct {
	Code     string `json:"code"`
	Message  string `json:"message,omitempty"`
	Revision int64  `json:"revision,omitempty"`
}

// writeError maps a domain or infrastructure error to a stable HTTP response
// (public interface 7). Operation, revision, and terminal conflicts use 409;
// other deterministic domain rejections use 422; malformed requests use 400;
// everything else is a 500.
func writeError(w http.ResponseWriter, err error) {
	var de *aliquot.Error
	if errors.As(err, &de) {
		writeJSON(w, statusForCode(de.Code), errorBody{Error: errorDetail{
			Code:     string(de.Code),
			Message:  de.Message,
			Revision: de.Revision,
		}})
		return
	}
	if isValidation(err) {
		writeJSON(w, http.StatusBadRequest, errorBody{Error: errorDetail{
			Code:    "BAD_REQUEST",
			Message: err.Error(),
		}})
		return
	}
	writeJSON(w, http.StatusInternalServerError, errorBody{Error: errorDetail{
		Code:    "INTERNAL",
		Message: err.Error(),
	}})
}

// statusForCode maps a stable domain code to an HTTP status.
func statusForCode(code aliquot.Code) int {
	switch code {
	case aliquot.CodeOperationConflict, aliquot.CodeRevisionConflict, aliquot.CodeTerminalConflict,
		aliquot.CodeMotherAlreadyReserved:
		return http.StatusConflict
	default:
		return http.StatusUnprocessableEntity
	}
}
