package fetcher

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"golang.org/x/crypto/blake2b"
)

// TestValidateSuccess covers the happy path.
func TestValidateSuccess(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes:           16,
		MaxBodyBytes:           1024,
		AllowedContentTypes:    []string{"application/json"},
		AllowTextHTMLWithBrace: true,
		JSON: JSONValidationConfig{
			RequiredFields:   []string{"name", "ticker"},
			MaxDepth:         4,
			MaxStringLength:  1024,
			MaxKeysPerObject: 32,
			MaxArrayLength:   32,
		},
		Hash: HashValidationConfig{Algorithm: "blake2b-256"},
	}
	body := []byte(`{"name":"StakeCo","ticker":"STAKE","description":"hi"}`)
	res, err := Validate(body, "application/json", nil, cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !bytes.Equal(res.Body, body) {
		t.Error("Body not preserved")
	}
	if !json.Valid(res.JSON) {
		t.Error("JSON output is not valid")
	}
	if len(res.MetaHash) != 32 {
		t.Errorf("MetaHash len = %d, want 32", len(res.MetaHash))
	}
}

// TestValidateMissingRequiredFields covers the CIP-6 check.
func TestValidateMissingRequiredFields(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes:        16,
		MaxBodyBytes:        1024,
		AllowedContentTypes: []string{"application/json"},
		JSON: JSONValidationConfig{
			RequiredFields: []string{"name", "ticker"},
			MaxDepth:       4, MaxStringLength: 1024, MaxKeysPerObject: 32, MaxArrayLength: 32,
		},
		Hash: HashValidationConfig{Algorithm: "blake2b-256"},
	}
	body := []byte(`{"name":"StakeCo"}`)
	_, err := Validate(body, "application/json", nil, cfg)
	if err == nil {
		t.Fatal("expected missing-fields error")
	}
	if !strings.Contains(err.Error(), "ticker") {
		t.Errorf("expected 'ticker' in error, got: %v", err)
	}
}

// TestValidateSizeLow covers the min_body_bytes rule.
func TestValidateSizeLow(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes: 100, MaxBodyBytes: 1024,
		AllowedContentTypes: []string{"application/json"},
		Hash:                HashValidationConfig{Algorithm: "blake2b-256"},
	}
	_, err := Validate([]byte(`{}`), "application/json", nil, cfg)
	if err == nil {
		t.Fatal("expected size-low error")
	}
}

// TestValidateSizeHigh covers the max_body_bytes rule.
func TestValidateSizeHigh(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes:        0,
		MaxBodyBytes:        10,
		AllowedContentTypes: []string{"application/json"},
		Hash:                HashValidationConfig{Algorithm: "blake2b-256"},
	}
	_, err := Validate([]byte(`{"a":"b","c":"d","e":"f"}`), "application/json", nil, cfg)
	if err == nil {
		t.Fatal("expected size-high error")
	}
}

// TestValidateContentType covers the allowlist.
func TestValidateContentType(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 1024,
		AllowedContentTypes:    []string{"application/json"},
		AllowTextHTMLWithBrace: false,
	}
	_, err := Validate([]byte(`{}`), "application/xml", nil, cfg)
	if err == nil {
		t.Fatal("expected content-type rejection")
	}
}

// TestValidateTextHTMLBraceFallback covers the legacy workaround.
func TestValidateTextHTMLBraceFallback(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes:           1,
		MaxBodyBytes:           1024,
		AllowedContentTypes:    []string{"application/json"},
		AllowTextHTMLWithBrace: true,
		JSON:                   noLimitJSONConfig(),
	}
	res, err := Validate([]byte(`{"a":1}`), "text/html", nil, cfg)
	if err != nil {
		t.Fatalf("text/html brace fallback should accept: %v", err)
	}
	if res == nil {
		t.Fatal("nil result")
	}
}

// TestValidateMaxDepth covers the JSON depth cap.
func TestValidateMaxDepth(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 4096,
		AllowedContentTypes: []string{"application/json"},
		JSON: JSONValidationConfig{
			MaxDepth: 3, MaxStringLength: 1024, MaxKeysPerObject: 32, MaxArrayLength: 32,
			RequiredFields: []string{},
		},
		Hash: HashValidationConfig{Algorithm: "blake2b-256"},
	}
	body := []byte(`{"a":{"b":{"c":{"d":1}}}}`)
	_, err := Validate(body, "application/json", nil, cfg)
	if err == nil {
		t.Fatal("expected depth-limit error")
	}
}

// TestValidateMaxStringLength covers per-string cap.
func TestValidateMaxStringLength(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 4096,
		AllowedContentTypes: []string{"application/json"},
		JSON: JSONValidationConfig{
			MaxDepth: 4, MaxStringLength: 5, MaxKeysPerObject: 32, MaxArrayLength: 32,
			RequiredFields: []string{},
		},
		Hash: HashValidationConfig{Algorithm: "blake2b-256"},
	}
	body := []byte(`{"x":"toolongstring"}`)
	_, err := Validate(body, "application/json", nil, cfg)
	if err == nil {
		t.Fatal("expected string-length error")
	}
}

// TestHashCorrect covers blake2b computation.
func TestHashCorrect(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	h, _ := blake2b.New256(nil)
	h.Write(body)
	want := h.Sum(nil)
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 4096,
		AllowedContentTypes: []string{"application/json"},
		JSON:                JSONValidationConfig{MaxDepth: 4, MaxStringLength: 1024, MaxKeysPerObject: 32, MaxArrayLength: 32},
		Hash:                HashValidationConfig{Algorithm: "blake2b-256"},
	}
	res, err := Validate(body, "application/json", want, cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if !bytes.Equal(res.MetaHash, want) {
		t.Error("hash mismatch on identical input")
	}
	if !res.HashMatched {
		t.Error("expected HashMatched=true on identical hash")
	}
}

// TestHashMismatchInformational covers the spec's no-rejection rule.
func TestHashMismatchInformational(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	h, _ := blake2b.New256(nil)
	h.Write(body)
	other := h.Sum(nil)
	for i := range other {
		other[i] ^= 0xff
	}
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 4096,
		AllowedContentTypes: []string{"application/json"},
		JSON:                noLimitJSONConfig(),
		Hash:                HashValidationConfig{Algorithm: "blake2b-256", LogMismatches: true},
	}
	res, err := Validate(body, "application/json", other, cfg)
	if err != nil {
		t.Fatalf("hash mismatch must not reject: %v", err)
	}
	if res.HashMatched {
		t.Error("expected HashMatched=false")
	}
	if bytes.Equal(res.MetaHash, other) {
		t.Error("expected MetaHash != other (mutated)")
	}
}

// TestHashNoneReturnsZeros covers the "none" algorithm opt-out.
func TestHashNoneReturnsZeros(t *testing.T) {
	cfg := ValidationConfig{
		MinBodyBytes: 1, MaxBodyBytes: 4096,
		AllowedContentTypes: []string{"application/json"},
		JSON:                noLimitJSONConfig(),
		Hash:                HashValidationConfig{Algorithm: "none"},
	}
	res, err := Validate([]byte(`{}`), "application/json", make([]byte, 32), cfg)
	if err != nil {
		t.Fatalf("Validate: %v", err)
	}
	for _, b := range res.MetaHash {
		if b != 0 {
			t.Error("expected 32 zero bytes for hash=none, got nonzero")
		}
	}
}

// noLimitJSONConfig returns a JSON config with permissive limits, suitable for
// tests that aren't exercising the JSON-shape validators.
func noLimitJSONConfig() JSONValidationConfig {
	return JSONValidationConfig{
		MaxDepth:              32,
		MaxStringLength:       65536,
		MaxKeysPerObject:      256,
		MaxArrayLength:        1024,
		RequiredFields:        []string{},
		AllowAdditionalFields: true,
	}
}
