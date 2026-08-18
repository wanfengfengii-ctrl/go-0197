package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// Limits for request parsing (public interface 7): unknown fields are rejected,
// request bodies are size-capped, and individual string lengths plus the child
// plan size are bounded.
const (
	maxBodyBytes    = 1 << 20 // 1 MiB
	maxStringLength = 256
	maxPlanChildren = 1000
)

// decode reads and decodes a JSON request body, rejecting unknown fields,
// oversized bodies, and trailing garbage (public interface 7).
func decode(r *http.Request, v any) error {
	data, err := io.ReadAll(io.LimitReader(r.Body, maxBodyBytes+1))
	if err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	if len(data) > maxBodyBytes {
		return fmt.Errorf("invalid request body: exceeds %d bytes", maxBodyBytes)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	if err := dec.Decode(&struct{}{}); err != io.EOF {
		return fmt.Errorf("invalid request body: unexpected trailing data")
	}
	return nil
}

// validString rejects strings longer than the configured limit.
func validString(s string) error {
	if len(s) > maxStringLength {
		return fmt.Errorf("string exceeds maximum length %d", maxStringLength)
	}
	return nil
}

// validationError summarizes a failed request-validation check.
type validationError struct{ msg string }

func (e *validationError) Error() string { return e.msg }

func failf(format string, args ...any) error {
	return &validationError{msg: fmt.Sprintf(format, args...)}
}

// validateStrings checks a set of request strings against the length limit.
func validateStrings(values ...string) error {
	for _, v := range values {
		if err := validString(v); err != nil {
			return err
		}
	}
	return nil
}

// isValidation reports whether an error is a request validation error (400).
func isValidation(err error) bool {
	_, ok := err.(*validationError)
	return ok || len(err.Error()) >= len("invalid request body") && err.Error()[:len("invalid request body")] == "invalid request body"
}
