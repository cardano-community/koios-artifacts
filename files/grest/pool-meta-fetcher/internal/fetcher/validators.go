// Package fetcher (validators.go) implements the response-validation pipeline
// from openspec/specs/pool-meta-fetcher-config/spec.md §fetch-validation.
//
// Validators run in order; the first failure aborts and returns the reason.
// Validators are pure functions of the bytes and metadata; they do not perform
// any I/O.
package fetcher

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"golang.org/x/crypto/blake2b"
)

// ValidationError carries the offending field and human-readable reason.
type ValidationError struct {
	Field  string
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation_failed field=%s reason=%q", e.Field, e.Reason)
}

// ValidationConfig is the subset of pool_meta_fetcher_config.ValidationConfig
// that validators need. Kept separate so validators don't import the config
// package (which would create a cycle with main).
type ValidationConfig struct {
	MinBodyBytes           int
	MaxBodyBytes           int
	AllowedContentTypes    []string
	AllowTextHTMLWithBrace bool
	JSON                   JSONValidationConfig
	Hash                   HashValidationConfig
}

type JSONValidationConfig struct {
	RequiredFields        []string
	MaxDepth              int
	MaxStringLength       int
	MaxKeysPerObject      int
	MaxArrayLength        int
	AllowAdditionalFields bool
}

type HashValidationConfig struct {
	Algorithm     string // "blake2b-256" or "none"
	LogMismatches bool
}

// ValidationResult is the output of a successful Fetch + Validate run.
type ValidationResult struct {
	Body        []byte
	JSON        []byte // json.Marshal of parsed JSON (round-tripped)
	MetaHash    []byte // 32 bytes for blake2b-256; 32 zero bytes if algorithm is "none"
	HashMatched bool   // informational only; never causes a rejection
}

// Validate runs the full pipeline. On success returns *ValidationResult.
// On failure returns a *ValidationError.
func Validate(body []byte, contentType string, onChainHash []byte, cfg ValidationConfig) (*ValidationResult, error) {
	if err := validateSize(body, cfg); err != nil {
		return nil, err
	}
	if err := validateContentType(body, contentType, cfg); err != nil {
		return nil, err
	}

	var parsed any
	if err := validateJSON(body, &parsed, cfg.JSON); err != nil {
		return nil, err
	}

	jsonBytes, err := json.Marshal(parsed)
	if err != nil {
		return nil, &ValidationError{Field: "json", Reason: "re-encode failed: " + err.Error()}
	}

	hash, matched := validateHash(body, onChainHash, cfg.Hash)
	return &ValidationResult{
		Body:        body,
		JSON:        jsonBytes,
		MetaHash:    hash,
		HashMatched: matched,
	}, nil
}

// -----------------------------------------------------------------------------
// Size
// -----------------------------------------------------------------------------

func validateSize(body []byte, cfg ValidationConfig) error {
	if len(body) < cfg.MinBodyBytes {
		return &ValidationError{
			Field:  "size",
			Reason: fmt.Sprintf("body too small: %d < min_body_bytes (%d)", len(body), cfg.MinBodyBytes),
		}
	}
	if len(body) > cfg.MaxBodyBytes {
		return &ValidationError{
			Field:  "size",
			Reason: fmt.Sprintf("body too large: %d > max_body_bytes (%d)", len(body), cfg.MaxBodyBytes),
		}
	}
	return nil
}

// -----------------------------------------------------------------------------
// Content-Type
// -----------------------------------------------------------------------------

func validateContentType(body []byte, contentType string, cfg ValidationConfig) error {
	ct := strings.ToLower(strings.TrimSpace(contentType))
	// Strip "; charset=utf-8" or similar parameters.
	if i := strings.IndexByte(ct, ';'); i >= 0 {
		ct = strings.TrimSpace(ct[:i])
	}
	if ct == "" {
		// Some SPOs omit Content-Type. Treat text/plain with leading brace as ok.
		ct = "text/plain"
	}

	for _, allowed := range cfg.AllowedContentTypes {
		if strings.EqualFold(ct, allowed) {
			return nil
		}
	}
	if ct == "text/html" && cfg.AllowTextHTMLWithBrace && bytes.HasPrefix(body, []byte("{")) {
		return nil
	}
	return &ValidationError{
		Field:  "content_type",
		Reason: fmt.Sprintf("disallowed: %q", contentType),
	}
}

// -----------------------------------------------------------------------------
// JSON shape
// -----------------------------------------------------------------------------

// validateJSON enforces structural rules: required fields (top-level),
// max depth, max string length, max keys per object, max array length, and
// whether additional fields are allowed (currently unused — informational).
//
// It is recursive but bounds itself via MaxDepth. It DOES NOT validate that
// required fields are non-empty by content (per spec — we accept empty
// strings; the user's RPC consumers can check themselves).
func validateJSON(body []byte, out *any, cfg JSONValidationConfig) error {
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return &ValidationError{Field: "json", Reason: "parse failed: " + err.Error()}
	}
	// Detect trailing data.
	if dec.More() {
		return &ValidationError{Field: "json", Reason: "trailing data after first JSON value"}
	}

	// Walk and enforce shape constraints.
	v := *out
	m, ok := v.(map[string]any)
	if !ok {
		return &ValidationError{Field: "json", Reason: "top-level is not an object"}
	}
	if len(m) > cfg.MaxKeysPerObject {
		return &ValidationError{
			Field:  "json",
			Reason: fmt.Sprintf("top-level has %d keys, exceeds max_keys_per_object (%d)", len(m), cfg.MaxKeysPerObject),
		}
	}
	for _, req := range cfg.RequiredFields {
		if _, present := m[req]; !present {
			return &ValidationError{
				Field:  "json.required_fields",
				Reason: fmt.Sprintf("missing required field: %s", req),
			}
		}
	}
	if err := walkShape(v, 1, cfg); err != nil {
		return err
	}
	return nil
}

// walkShape descends recursively, enforcing MaxDepth, MaxStringLength, MaxKeysPerObject, MaxArrayLength.
func walkShape(v any, depth int, cfg JSONValidationConfig) error {
	if depth > cfg.MaxDepth {
		return &ValidationError{
			Field:  "json",
			Reason: fmt.Sprintf("depth %d exceeds max_depth (%d)", depth, cfg.MaxDepth),
		}
	}
	switch t := v.(type) {
	case map[string]any:
		if len(t) > cfg.MaxKeysPerObject {
			return &ValidationError{
				Field:  "json",
				Reason: fmt.Sprintf("object at depth %d has %d keys, exceeds max_keys_per_object (%d)", depth, len(t), cfg.MaxKeysPerObject),
			}
		}
		for _, vv := range t {
			if err := walkShape(vv, depth+1, cfg); err != nil {
				return err
			}
		}
	case []any:
		if len(t) > cfg.MaxArrayLength {
			return &ValidationError{
				Field:  "json",
				Reason: fmt.Sprintf("array at depth %d has %d elements, exceeds max_array_length (%d)", depth, len(t), cfg.MaxArrayLength),
			}
		}
		for _, vv := range t {
			if err := walkShape(vv, depth+1, cfg); err != nil {
				return err
			}
		}
	case string:
		if len(t) > cfg.MaxStringLength {
			return &ValidationError{
				Field:  "json",
				Reason: fmt.Sprintf("string at depth %d has length %d, exceeds max_string_length (%d)", depth, len(t), cfg.MaxStringLength),
			}
		}
	case nil, bool, json.Number, float64:
		// primitive leaf
	}
	return nil
}

// -----------------------------------------------------------------------------
// Hash
// -----------------------------------------------------------------------------

// validateHash computes Blake2b-256 of body and compares against onChainHash
// (if any). The boolean return is informational; the spec mandates never
// rejecting on mismatch.
func validateHash(body, onChainHash []byte, cfg HashValidationConfig) ([]byte, bool) {
	if cfg.Algorithm == "none" {
		return make([]byte, 32), false
	}
	// Default / blake2b-256
	h, err := blake2b.New256(nil)
	if err != nil {
		return make([]byte, 32), false
	}
	_, _ = h.Write(body)
	hash := h.Sum(nil)
	if len(onChainHash) != 32 {
		return hash, false
	}
	return hash, bytes.Equal(hash, onChainHash)
}

// -----------------------------------------------------------------------------
// Errors helpers
// -----------------------------------------------------------------------------

// IsValidation returns true if err is a *ValidationError.
func IsValidation(err error) bool {
	var e *ValidationError
	return errors.As(err, &e)
}
