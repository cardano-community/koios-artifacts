// fetcher unit tests (no DB / no network). The integration tests in
// integration_test.go require KOIOS_PG_TEST_DSN.
package fetcher

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/config"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/storage"
)

// newCfg returns a Config with sensible defaults for filter tests; only the
// WorkQueue allow/deny lists are set per-test.
func newCfg() *config.Config {
	return &config.Config{}
}

// TestApplyFilterListsEmptyLists is the default path: empty allow+deny
// means every work item passes through.
func TestApplyFilterListsEmptyLists(t *testing.T) {
	f := &Fetcher{}
	f.cfg.Store(newCfg())

	items := []storage.WorkItem{
		{PoolID: 1, Bech32: "pool1abc"},
		{PoolID: 2, Bech32: "pool1def"},
	}
	got := f.applyFilterLists(items)
	if len(got) != 2 {
		t.Fatalf("expected 2 items through, got %d", len(got))
	}
	for i, wi := range got {
		if wi.Bech32 != items[i].Bech32 {
			t.Errorf("item %d bech32 lost: got %q want %q", i, wi.Bech32, items[i].Bech32)
		}
	}
}

// TestApplyFilterListsAllowDeny covers both lists and the overlap rule
// (deny wins).
func TestApplyFilterListsAllowDeny(t *testing.T) {
	cfg := newCfg()
	cfg.Fetch.WorkQueue.PoolIDAllowList = []string{"pool1abc", "pool1def"}
	cfg.Fetch.WorkQueue.PoolIDDenyList = []string{"pool1def"}
	f := &Fetcher{}
	f.cfg.Store(cfg)

	items := []storage.WorkItem{
		{PoolID: 1, Bech32: "pool1abc"}, // allowed
		{PoolID: 2, Bech32: "pool1def"}, // in both → deny wins → excluded
		{PoolID: 3, Bech32: "pool1xyz"}, // not in allow → excluded
	}
	got := f.applyFilterLists(items)
	if len(got) != 1 {
		t.Fatalf("expected 1 item through, got %d", len(got))
	}
	if got[0].Bech32 != "pool1abc" {
		t.Errorf("got %q, want pool1abc", got[0].Bech32)
	}
}

// TestApplyFilterListsGlob covers glob entries in allow_list / deny_list.
func TestApplyFilterListsGlob(t *testing.T) {
	cfg := newCfg()
	cfg.Fetch.WorkQueue.PoolIDAllowList = []string{"pool1abc*"}
	cfg.Fetch.WorkQueue.PoolIDDenyList = []string{"pool1abcdef*"}
	f := &Fetcher{}
	f.cfg.Store(cfg)

	items := []storage.WorkItem{
		{PoolID: 1, Bech32: "pool1abcXXX"},    // allowed (glob match)
		{PoolID: 2, Bech32: "pool1abcdefZZZ"}, // in both → deny wins
		{PoolID: 3, Bech32: "pool1qqq"},       // not in allow
	}
	got := f.applyFilterLists(items)
	if len(got) != 1 || got[0].Bech32 != "pool1abcXXX" {
		t.Errorf("glob filter wrong: %v", got)
	}
}

// TestPoolMatchedCaseInsensitive asserts the comparison is lowercase.
func TestPoolMatchedCaseInsensitive(t *testing.T) {
	if !poolMatched("pool1ABC", []string{"pool1abc"}) {
		t.Error("expected case-insensitive match")
	}
	if poolMatched("pool1zzz", []string{"pool1abc"}) {
		t.Error("expected non-match")
	}
}

// TestIsGlob covers the metacharacter probe.
func TestIsGlob(t *testing.T) {
	for _, c := range []struct {
		in   string
		want bool
	}{
		{"pool1abc", false},
		{"pool1abc*", true},
		{"pool1a?c", true},
		{"pool1a[bc]c", true},
		{"", false},
	} {
		if got := isGlob(c.in); got != c.want {
			t.Errorf("isGlob(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// TestLogFilterConfig exercises all three spec-mandated log shapes.
func TestLogFilterConfig(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	LogFilterConfig(log, nil, []string{"pool1def", "pool1ghi"})
	if !strings.Contains(buf.String(), "pools_excluded") {
		t.Errorf("expected pools_excluded, got: %s", buf.String())
	}

	buf.Reset()
	LogFilterConfig(log, []string{"pool1a", "pool1b"}, []string{"pool1b"})
	out := buf.String()
	if !strings.Contains(out, "config_overlap pool_id=pool1b") {
		t.Errorf("expected config_overlap, got: %s", out)
	}

	buf.Reset()
	deny := make([]string, 0, 101)
	for i := 0; i < 101; i++ {
		deny = append(deny, "pool1z"+string(rune('a'+i%26)))
	}
	LogFilterConfig(log, nil, deny)
	if !strings.Contains(buf.String(), "large_deny_list") {
		t.Errorf("expected large_deny_list, got: %s", buf.String())
	}
}

// TestOverlapExactExcludesGlobs makes sure glob entries are not reported
// as overlaps (their matches can't be enumerated statically).
func TestOverlapExactExcludesGlobs(t *testing.T) {
	got := overlapExact([]string{"pool1abc", "pool1xyz*"}, []string{"pool1abc", "pool1xyz*"})
	if len(got) != 1 || got[0] != "pool1abc" {
		t.Errorf("overlapExact = %v, want [pool1abc]", got)
	}
}

// TestPoolFetcherRetrySnapshot covers the backoff curve exposed via
// /debug/pools.
func TestPoolFetcherRetrySnapshot(t *testing.T) {
	pf := &PoolFetcher{
		retries:  map[int64]uint{1: 1, 2: 4},
		mismatch: map[int64]bool{},
	}
	snap := pf.RetrySnapshot(60)
	if len(snap) != 2 {
		t.Fatalf("expected 2 retry entries, got %d", len(snap))
	}
	// Curve mirrors the DB SQL: base * 2^(consecutive_failures-1).
	// consecutive_failures=1 → 60 * 2^0 = 60s (1m)
	// consecutive_failures=4 → 60 * 2^3 = 480s (8m)
	var saw1, saw4 bool
	for _, e := range snap {
		switch e.Attempts {
		case 1:
			saw1 = true
			if e.NextDelay != 60*time.Second {
				t.Errorf("attempt=1 delay = %v, want 60s", e.NextDelay)
			}
		case 4:
			saw4 = true
			if e.NextDelay != 480*time.Second {
				t.Errorf("attempt=4 delay = %v, want 480s", e.NextDelay)
			}
		}
	}
	if !saw1 || !saw4 {
		t.Errorf("missing attempt entry: snap=%+v", snap)
	}
}

// TestRetryDelayCap asserts the curve caps at the SQL's
// LEAST(consecutive_failures, 10) guard.
func TestRetryDelayCap(t *testing.T) {
	// consecutive_failures=20 (post-increment) → pre-increment read = 19 →
	// SQL computes pow(2, LEAST(19, 10)) = 1024. So 60 * 1024 ≈ 17 h is the cap.
	got := retryDelay(60, 20)
	want := 60 * time.Second * 1024
	if got != want {
		t.Errorf("retryDelay(60, 20) = %v, want clamped at %v", got, want)
	}
}
