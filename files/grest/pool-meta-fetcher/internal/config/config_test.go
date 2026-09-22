package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// TestLoadMissing ensures Load() returns a clear error when path is absent.
func TestLoadMissing(t *testing.T) {
	dir := t.TempDir()
	_, err := Load(filepath.Join(dir, "nope.yaml"), nil)
	if err == nil {
		t.Fatal("expected error for missing config path")
	}
}

// TestLoadMinimal ensures the two-field minimal config loads and validates.
func TestLoadMinimal(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	mustWrite(t, path, `
database_url: postgres://grest_owner:xxx@localhost:5432/cexplorer
network_name: mainnet
`)
	cfg, err := Load(path, nil)
	if err != nil {
		t.Fatalf("Load returned: %v", err)
	}
	if cfg.NetworkName != NetworkMainnet {
		t.Errorf("network_name = %q, want mainnet", cfg.NetworkName)
	}
	if !cfg.Fetch.Enabled {
		t.Error("fetch.enabled default should be true")
	}
	if cfg.Fetch.Validation.MaxBodyBytes != 524288 {
		t.Errorf("max_body_bytes = %d, want 524288", cfg.Fetch.Validation.MaxBodyBytes)
	}
}

// TestUnknownFieldRejected covers rule "dec.KnownFields(true)".
func TestUnknownFieldRejected(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	mustWrite(t, path, `
database_url: postgres://grest_owner:xxx@localhost:5432/cexplorer
network_name: mainnet
fetch:
  totally_made_up_knob: 42
`)
	_, err := Load(path, nil)
	if err == nil {
		t.Fatal("expected error for unknown field")
	}
	if !strings.Contains(err.Error(), "parse yaml") {
		t.Errorf("err = %v, expected parse-yaml failure", err)
	}
}

// TestAllValidationRules fires every documented rule with each shape of
// bad input and confirms a typed *ValidationError is returned pointing at
// the right field.
func TestAllValidationRules(t *testing.T) {
	// valid pool bech32 for tests; matches the regex pool1[ac-hij-np-z02-9]{20,}
	const validPool = "pool1q2w3e4r5t6y7u8i9aqwerty"

	type tc struct {
		name      string
		override  string
		wantField string
	}
	cases := []tc{
		// 1 database_url required — omit entirely from the yaml
		{"missing database_url", "\nnetwork_name: mainnet\n", "database_url"},
		// 2 network_name enum
		{"bad network_name", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: bogus\n", "network_name"},
		// 3 poll_interval_seconds
		{"poll <= 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  poll:\n    poll_interval_seconds: 0\n", "fetch.poll.poll_interval_seconds"},
		// 4 min_poll > poll
		{"min > poll", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  poll:\n    poll_interval_seconds: 100\n    min_poll_interval_seconds: 200\n", "fetch.poll.min_poll_interval_seconds"},
		// 5 concurrency
		{"concurrency 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  work_queue:\n    concurrency: 0\n", "fetch.work_queue.concurrency"},
		// 6 batch_size 0
		{"batch_size 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  work_queue:\n    batch_size: 0\n", "fetch.work_queue.batch_size"},
		// 7 max_inflight < concurrency
		{"max_inflight < concurrency", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  work_queue:\n    concurrency: 16\n    max_inflight: 8\n", "fetch.work_queue.max_inflight"},
		// 8 write_batch_size 0
		{"write_batch_size 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  work_queue:\n    write_batch_size: 0\n", "fetch.work_queue.write_batch_size"},
		// 9 http timeout
		{"connect > timeout", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  http:\n    timeout_seconds: 10\n    connect_timeout_seconds: 30\n", "fetch.http.connect_timeout_seconds"},
		// 10 max_redirects
		{"max_redirects negative", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  http:\n    max_redirects: -1\n", "fetch.http.max_redirects"},
		// 11 tls_min_version
		{"tls_min_version bogus", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  http:\n    tls_min_version: 1.1\n", "fetch.http.tls_min_version"},
		// 12 max_body_bytes too large
		{"max_body 5MiB+1", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  validation:\n    max_body_bytes: 5242881\n", "fetch.validation.max_body_bytes"},
		// 13 allowed_content_types empty
		{"allowed_content_types empty", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  validation:\n    allowed_content_types: []\n", "fetch.validation.allowed_content_types"},
		// 14 json max_depth <= 0
		{"json max_depth 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  validation:\n    json:\n      max_depth: 0\n", "fetch.validation.json.max_depth"},
		// 15 hash algorithm bogus
		{"hash algo bogus", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  validation:\n    hash:\n      algorithm: sha256\n", "fetch.validation.hash.algorithm"},
		// 16 allow_list with empty pools
		{"allow_list policy with empty list", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  private_ip_policy: allow_list\n  private_ip_allow_list: []\n", "fetch.private_ip_allow_list"},
		// 17 retry max_attempts 0
		{"retry max_attempts 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  retry:\n    max_attempts: 0\n", "fetch.retry.max_attempts"},
		// 18 pool_max_conns 0
		{"pool_max_conns 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\ndatabase:\n  pool_max_conns: 0\n", "database.pool_max_conns"},
		// 19 pool_min_conns > max
		{"pool_min > max", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\ndatabase:\n  pool_min_conns: 20\n  pool_max_conns: 10\n", "database.pool_min_conns"},
		// 20 statement_timeout_ms 0
		{"statement_timeout_ms 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\ndatabase:\n  statement_timeout_ms: 0\n", "database.statement_timeout_ms"},
		// 23 health.listen_port
		{"listen_port 70000", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nhealth:\n  listen_port: 70000\n", "health.listen_port"},
		// 24 health.listen_addr empty
		{"listen_addr empty", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nhealth:\n  listen_addr: \"\"\n", "health.listen_addr"},
		// 25 readyz_db_grace_seconds 0
		{"readyz_grace 0", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nhealth:\n  readyz_db_grace_seconds: 0\n", "health.readyz_db_grace_seconds"},
		// 26 log level bogus
		{"log level bogus", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nlogging:\n  level: trace\n", "logging.level"},
		// 27 log format bogus
		{"log format bogus", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nlogging:\n  format: yaml\n", "logging.format"},
		// 28 pools_overrides_match_style bogus
		{"match style bogus", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  pools_overrides_match_style: regex\n", "fetch.pools_overrides_match_style"},
		// 29 pools_overrides unknown key (use glob match to bypass bech32 check first)
		{"override unknown key", "\ndatabase_url: postgres://grest_owner:xxx@localhost:5432/cexplorer\nnetwork_name: mainnet\nfetch:\n  pools_overrides:\n    - match: \"*test*\"\n      overrides:\n        unknown_knob: 1\n", "fetch.pools_overrides[0].overrides"},
	}

	_ = validPool // referenced only in TestPoolsOverridesAllowedKeys

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "c.yaml")
			mustWrite(t, path, c.override)
			_, err := Load(path, nil)
			if err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
			ve, ok := err.(*ValidationError)
			if ok && ve.Field != c.wantField {
				t.Errorf("err.Field = %q, want %q (%v)", ve.Field, c.wantField, err)
			} else if !ok {
				if !strings.Contains(err.Error(), c.wantField) {
					t.Errorf("err = %v, expected to contain field %q", err, c.wantField)
				}
			}
		})
	}
}

// TestPoolsOverridesAllowedKeys ensures the spec's strict allowlist.
func TestPoolsOverridesAllowedKeys(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.yaml")
	mustWrite(t, path, `
database_url: postgres://grest_owner:xxx@localhost:5432/cexplorer
network_name: mainnet
fetch:
  pools_overrides:
    - match: "*pool1q2w3e4r5t6y7u8i9aqwerty*"
      overrides:
        max_body_bytes: 1048576
        timeout_seconds: 60
        user_agent: "x"
        private_ip_policy: "allow_all"
        enabled: false
`)
	if _, err := Load(path, nil); err != nil {
		t.Fatalf("expected all-override keys to validate, got %v", err)
	}
}

// TestPollInterval returns the expected Duration.
func TestPollIntervalAccessor(t *testing.T) {
	c := defaults()
	c.Fetch.Poll.PollIntervalSeconds = 600
	if got := c.PollInterval(); got != 600*time.Second {
		t.Errorf("PollInterval() = %v, want 600s", got)
	}
}

// helpers ----------------------------------------------------------------------

func mustWrite(t *testing.T, path, body string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
}
