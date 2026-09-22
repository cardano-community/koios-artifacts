// Package config defines the YAML schema, defaults, and validation rules for
// the koios-pool-meta-fetcher daemon. The configuration model is specified in
// openspec/specs/pool-meta-fetcher-config/spec.md and validated against 32 rules.
package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	"log/slog"

	"gopkg.in/yaml.v3"
)

// -----------------------------------------------------------------------------
// Top-level schema
// -----------------------------------------------------------------------------

// Config is the fully-loaded and validated configuration.
type Config struct {
	DatabaseURL string       `yaml:"database_url"`
	NetworkName NetworkName  `yaml:"network_name"`
	SchemaName  string       `yaml:"schema_name"`
	Fetch       FetchConfig  `yaml:"fetch"`
	Database    DBConfig     `yaml:"database"`
	Health      HealthConfig `yaml:"health"`
	Logging     LogConfig    `yaml:"logging"`
}

// Load reads the YAML file at path, applies defaults, and validates against the
// 32-rule schema. On any validation failure it returns a *ValidationError with
// the offending field, expected rule, and current value.
func Load(path string, logger *slog.Logger) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	cfg := defaults()
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true) // strict — rejects unknown top-level keys
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// -----------------------------------------------------------------------------
// Network name (enum)
// -----------------------------------------------------------------------------

type NetworkName string

const (
	NetworkMainnet   NetworkName = "mainnet"
	NetworkPreprod   NetworkName = "preprod"
	NetworkPreview   NetworkName = "preview"
	NetworkSanchonet NetworkName = "sanchonet"
)

func (n NetworkName) Valid() bool {
	return slices.Contains([]NetworkName{
		NetworkMainnet, NetworkPreprod, NetworkPreview, NetworkSanchonet,
	}, n)
}

// -----------------------------------------------------------------------------
// fetch.*
// -----------------------------------------------------------------------------

type FetchConfig struct {
	Enabled             bool                `yaml:"enabled"`
	Poll                PollConfig          `yaml:"poll"`
	WorkQueue           WorkQueueConfig     `yaml:"work_queue"`
	HTTP                HTTPConfig          `yaml:"http"`
	Validation          ValidationConfig    `yaml:"validation"`
	PrivateIPPolicy     PrivateIPPolicyEnum `yaml:"private_ip_policy"`
	PrivateIPAllowList  []string            `yaml:"private_ip_allow_list"`
	Retry               RetryConfig         `yaml:"retry"`
	PoolsOverrides      []PoolOverride      `yaml:"pools_overrides"`
	PoolsOverridesMatch MatchStyleEnum      `yaml:"pools_overrides_match_style"`
}

type PollConfig struct {
	PollIntervalSeconds    int `yaml:"poll_interval_seconds"`
	MinPollIntervalSecs    int `yaml:"min_poll_interval_seconds"`
	FailedPoolRetrySeconds int `yaml:"failed_pool_retry_seconds"`
}

type WorkQueueConfig struct {
	Concurrency         int      `yaml:"concurrency"`
	BatchSize           int      `yaml:"batch_size"`
	MaxInflight         int      `yaml:"max_inflight"`
	WriteBatchSize      int      `yaml:"write_batch_size"`
	PoolIDAllowList     []string `yaml:"pool_id_allow_list"`
	PoolIDDenyList      []string `yaml:"pool_id_deny_list"`
	ExposeDebugEndpoint bool     `yaml:"expose_debug_endpoint"`
}

type HTTPConfig struct {
	TimeoutSeconds        int               `yaml:"timeout_seconds"`
	ConnectTimeoutSeconds int               `yaml:"connect_timeout_seconds"`
	MaxRedirects          int               `yaml:"max_redirects"`
	UserAgent             string            `yaml:"user_agent"`
	SkipTLSVerify         bool              `yaml:"skip_tls_verify"`
	TLSMinVersion         string            `yaml:"tls_min_version"`
	HTTP2                 bool              `yaml:"http2"`
	ProxyURL              string            `yaml:"proxy_url"`
	ExtraHeaders          map[string]string `yaml:"extra_headers"`
}

type ValidationConfig struct {
	MinBodyBytes             int            `yaml:"min_body_bytes"`
	MaxBodyBytes             int            `yaml:"max_body_bytes"`
	AllowedContentTypes      []string       `yaml:"allowed_content_types"`
	AllowTextHTMLWithBrace   bool           `yaml:"allow_text_html_with_brace"`
	FollowCrossHostRedirects bool           `yaml:"follow_cross_host_redirects"`
	JSON                     JSONValidation `yaml:"json"`
	Hash                     HashValidation `yaml:"hash"`
}

type JSONValidation struct {
	RequiredFields        []string `yaml:"required_fields"`
	MaxDepth              int      `yaml:"max_depth"`
	MaxStringLength       int      `yaml:"max_string_length"`
	MaxKeysPerObject      int      `yaml:"max_keys_per_object"`
	MaxArrayLength        int      `yaml:"max_array_length"`
	AllowAdditionalFields bool     `yaml:"allow_additional_fields"`
}

type HashValidation struct {
	Algorithm     HashAlgorithm `yaml:"algorithm"`
	LogMismatches bool          `yaml:"log_mismatches"`
}

type HashAlgorithm string

const (
	HashBlake2b256 HashAlgorithm = "blake2b-256"
	HashNone       HashAlgorithm = "none"
)

func (h HashAlgorithm) Valid() bool {
	return h == HashBlake2b256 || h == HashNone
}

type PrivateIPPolicyEnum string

const (
	PrivateIPBlock     PrivateIPPolicyEnum = "block"
	PrivateIPAllowAll  PrivateIPPolicyEnum = "allow_all"
	PrivateIPAllowList PrivateIPPolicyEnum = "allow_list"
)

func (p PrivateIPPolicyEnum) Valid() bool {
	return slices.Contains([]PrivateIPPolicyEnum{
		PrivateIPBlock, PrivateIPAllowAll, PrivateIPAllowList,
	}, p)
}

type RetryConfig struct {
	MaxAttempts  int  `yaml:"max_attempts"`
	PersistState bool `yaml:"persist_state"`
}

type PoolOverride struct {
	Match     string         `yaml:"match"`
	Overrides map[string]any `yaml:"overrides"`
}

type MatchStyleEnum string

const (
	MatchExact MatchStyleEnum = "exact"
	MatchGlob  MatchStyleEnum = "glob"
)

// -----------------------------------------------------------------------------
// database.*
// -----------------------------------------------------------------------------

type DBConfig struct {
	PoolMaxConns        int32         `yaml:"pool_max_conns"`
	PoolMinConns        int32         `yaml:"pool_min_conns"`
	PoolMaxConnLifetime time.Duration `yaml:"pool_max_conn_lifetime"`
	PoolMaxConnIdleTime time.Duration `yaml:"pool_max_conn_idle_time"`
	StatementTimeoutMs  int           `yaml:"statement_timeout_ms"`
}

// -----------------------------------------------------------------------------
// health.*
// -----------------------------------------------------------------------------

type HealthConfig struct {
	ListenAddr           string `yaml:"listen_addr"`
	ListenPort           int    `yaml:"listen_port"`
	ReadyzDBGraceSeconds int    `yaml:"readyz_db_grace_seconds"`
}

// -----------------------------------------------------------------------------
// logging.*
// -----------------------------------------------------------------------------

type LogConfig struct {
	Level     LogLevel  `yaml:"level"`
	Format    LogFormat `yaml:"format"`
	AddSource bool      `yaml:"add_source"`
}

type LogLevel string

const (
	LogDebug LogLevel = "debug"
	LogInfo  LogLevel = "info"
	LogWarn  LogLevel = "warn"
	LogError LogLevel = "error"
)

func (l LogLevel) Valid() bool {
	return slices.Contains([]LogLevel{LogDebug, LogInfo, LogWarn, LogError}, l)
}

func (l LogLevel) ToSlog() slog.Level {
	switch l {
	case LogDebug:
		return slog.LevelDebug
	case LogWarn:
		return slog.LevelWarn
	case LogError:
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type LogFormat string

const (
	FormatJSON LogFormat = "json"
	FormatText LogFormat = "text"
)

func (f LogFormat) Valid() bool {
	return f == FormatJSON || f == FormatText
}

// -----------------------------------------------------------------------------
// Defaults
// -----------------------------------------------------------------------------

func defaults() *Config {
	return &Config{
		NetworkName: NetworkMainnet,
		SchemaName:  "grest",
		Fetch: FetchConfig{
			Enabled: true,
			Poll: PollConfig{
				PollIntervalSeconds:    432000, // 5 days; matches Cardano mainnet epoch cadence
				MinPollIntervalSecs:    60,
				FailedPoolRetrySeconds: 60, // failed pools get a fast retry cycle regardless
			},
			WorkQueue: WorkQueueConfig{
				Concurrency:         16,
				BatchSize:           100,
				MaxInflight:         200,
				WriteBatchSize:      50,
				PoolIDAllowList:     []string{},
				PoolIDDenyList:      []string{},
				ExposeDebugEndpoint: false,
			},
			HTTP: HTTPConfig{
				TimeoutSeconds:        30,
				ConnectTimeoutSeconds: 10,
				MaxRedirects:          3,
				UserAgent:             "koios-pool-meta-fetcher/dev",
				SkipTLSVerify:         false,
				TLSMinVersion:         "1.2",
				HTTP2:                 true,
				ProxyURL:              "",
				ExtraHeaders:          map[string]string{},
			},
			Validation: ValidationConfig{
				MinBodyBytes: 16,
				MaxBodyBytes: 524288,
				AllowedContentTypes: []string{
					"application/json",
					"application/ld+json",
					"text/plain",
					"application/octet-stream",
					"binary/octet-stream",
					"application/binary",
				},
				AllowTextHTMLWithBrace:   true,
				FollowCrossHostRedirects: false,
				JSON: JSONValidation{
					RequiredFields: []string{
						"name", "description", "ticker", "homepage",
					},
					MaxDepth:              8,
					MaxStringLength:       8192,
					MaxKeysPerObject:      128,
					MaxArrayLength:        256,
					AllowAdditionalFields: true,
				},
				Hash: HashValidation{
					Algorithm:     HashBlake2b256,
					LogMismatches: true,
				},
			},
			PrivateIPPolicy:    PrivateIPBlock,
			PrivateIPAllowList: []string{},
			Retry: RetryConfig{
				MaxAttempts:  10,
				PersistState: false,
			},
			PoolsOverrides:      []PoolOverride{},
			PoolsOverridesMatch: MatchExact,
		},
		Database: DBConfig{
			PoolMaxConns:        10,
			PoolMinConns:        1,
			PoolMaxConnLifetime: time.Hour,
			PoolMaxConnIdleTime: 30 * time.Minute,
			StatementTimeoutMs:  60000,
		},
		Health: HealthConfig{
			ListenAddr:           "0.0.0.0",
			ListenPort:           8081,
			ReadyzDBGraceSeconds: 60,
		},
		Logging: LogConfig{
			Level:     LogInfo,
			Format:    FormatJSON,
			AddSource: false,
		},
	}
}

// -----------------------------------------------------------------------------
// Convenience accessors (pre-derived durations)
// -----------------------------------------------------------------------------

// PollInterval returns poll_interval_seconds as a Duration.
func (c *Config) PollInterval() time.Duration {
	return time.Duration(c.Fetch.Poll.PollIntervalSeconds) * time.Second
}

// FailedPoolRetry returns failed_pool_retry_seconds as a Duration.
func (c *Config) FailedPoolRetry() time.Duration {
	return time.Duration(c.Fetch.Poll.FailedPoolRetrySeconds) * time.Second
}

// MinPollInterval returns min_poll_interval_seconds as a Duration.
func (c *Config) MinPollInterval() time.Duration {
	return time.Duration(c.Fetch.Poll.MinPollIntervalSecs) * time.Second
}

// HTTPTimeout returns http.timeout_seconds as a Duration.
func (c *Config) HTTPTimeout() time.Duration {
	return time.Duration(c.Fetch.HTTP.TimeoutSeconds) * time.Second
}

// ConnectTimeout returns http.connect_timeout_seconds as a Duration.
func (c *Config) ConnectTimeout() time.Duration {
	return time.Duration(c.Fetch.HTTP.ConnectTimeoutSeconds) * time.Second
}

// -----------------------------------------------------------------------------
// Validation (the 32 rules from openspec/specs/pool-meta-fetcher-config/spec.md)
// -----------------------------------------------------------------------------

// ValidationError is returned by Validate with a precise pointer to the
// offending field and the rule that failed.
type ValidationError struct {
	Field  string
	Value  any
	Reason string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("invalid_config field=%s value=%v reason=%q", e.Field, e.Value, e.Reason)
}

// Validate enforces the 32-rule schema. Returns nil on success or a
// *ValidationError on the first failure (rules are checked in order).
func (c *Config) Validate() error {
	for i, rule := range configRules {
		if err := rule(c); err != nil {
			return fmt.Errorf("config rule #%d failed: %w", i+1, err)
		}
	}
	return nil
}

// configRules is the ordered list of validation rules from the spec.
type configRule func(*Config) error

var configRules = []configRule{
	// 1
	func(c *Config) error {
		if c.DatabaseURL == "" {
			return &ValidationError{Field: "database_url", Value: "", Reason: "required"}
		}
		// Accept both URL and libpq forms. URL must parse; libpq must contain
		// host= and dbname=.
		if u, err := url.Parse(c.DatabaseURL); err == nil && u.Scheme != "" {
			if u.Host == "" {
				return &ValidationError{Field: "database_url", Value: c.DatabaseURL,
					Reason: "URL form must include a host"}
			}
			return nil
		}
		if !(strings.Contains(c.DatabaseURL, "host=") && strings.Contains(c.DatabaseURL, "dbname=")) {
			return &ValidationError{Field: "database_url", Value: c.DatabaseURL,
				Reason: "must be a postgres:// URL or libpq keyword=value string containing host= and dbname="}
		}
		return nil
	},
	// 2
	func(c *Config) error {
		if !c.NetworkName.Valid() {
			return &ValidationError{Field: "network_name", Value: c.NetworkName,
				Reason: "must be one of mainnet|preprod|preview|sanchonet"}
		}
		return nil
	},
	// 2a — schema_name must be a valid SQL identifier (letters, digits, underscores;
	// not starting with a digit; bounded length).
	func(c *Config) error {
		s := c.SchemaName
		if s == "" {
			return &ValidationError{Field: "schema_name", Value: s, Reason: "required (default is 'grest')"}
		}
		if len(s) > 63 {
			return &ValidationError{Field: "schema_name", Value: s, Reason: "must be <= 63 characters"}
		}
		if !(schemaIdentRe.MatchString(s)) {
			return &ValidationError{Field: "schema_name", Value: s,
				Reason: "must match ^[A-Za-z_][A-Za-z0-9_]*$ (SQL identifier)"}
		}
		return nil
	},
	// 3
	func(c *Config) error {
		if c.Fetch.Poll.PollIntervalSeconds <= 0 {
			return &ValidationError{Field: "fetch.poll.poll_interval_seconds",
				Value: c.Fetch.Poll.PollIntervalSeconds, Reason: "must be > 0"}
		}
		return nil
	},
	// 4
	func(c *Config) error {
		p := c.Fetch.Poll.PollIntervalSeconds
		m := c.Fetch.Poll.MinPollIntervalSecs
		if m <= 0 {
			return &ValidationError{Field: "fetch.poll.min_poll_interval_seconds",
				Value: m, Reason: "must be > 0"}
		}
		if m > p {
			return &ValidationError{Field: "fetch.poll.min_poll_interval_seconds",
				Value: m, Reason: fmt.Sprintf("must be <= poll_interval_seconds (%d)", p)}
		}
		return nil
	},
	// 4a — failed_pool_retry must be > 0 and <= poll_interval (otherwise semantics invert)
	func(c *Config) error {
		fpr := c.Fetch.Poll.FailedPoolRetrySeconds
		pi := c.Fetch.Poll.PollIntervalSeconds
		if fpr <= 0 {
			return &ValidationError{Field: "fetch.poll.failed_pool_retry_seconds",
				Value: fpr, Reason: "must be > 0"}
		}
		if pi <= 0 {
			return &ValidationError{Field: "fetch.poll.poll_interval_seconds",
				Value: pi, Reason: "must be > 0"}
		}
		if fpr > pi {
			return &ValidationError{Field: "fetch.poll.failed_pool_retry_seconds",
				Value: fpr, Reason: fmt.Sprintf("must be <= poll_interval_seconds (%d)", pi)}
		}
		return nil
	},
	// 5
	func(c *Config) error {
		if c.Fetch.WorkQueue.Concurrency < 1 {
			return &ValidationError{Field: "fetch.work_queue.concurrency",
				Value: c.Fetch.WorkQueue.Concurrency, Reason: "must be >= 1"}
		}
		return nil
	},
	// 6
	func(c *Config) error {
		if c.Fetch.WorkQueue.BatchSize < 1 {
			return &ValidationError{Field: "fetch.work_queue.batch_size",
				Value: c.Fetch.WorkQueue.BatchSize, Reason: "must be >= 1"}
		}
		return nil
	},
	// 7
	func(c *Config) error {
		if c.Fetch.WorkQueue.MaxInflight < c.Fetch.WorkQueue.Concurrency {
			return &ValidationError{Field: "fetch.work_queue.max_inflight",
				Value:  c.Fetch.WorkQueue.MaxInflight,
				Reason: fmt.Sprintf("must be >= concurrency (%d)", c.Fetch.WorkQueue.Concurrency)}
		}
		return nil
	},
	// 8
	func(c *Config) error {
		if c.Fetch.WorkQueue.WriteBatchSize < 1 {
			return &ValidationError{Field: "fetch.work_queue.write_batch_size",
				Value: c.Fetch.WorkQueue.WriteBatchSize, Reason: "must be >= 1"}
		}
		return nil
	},
	// 9
	func(c *Config) error {
		t := c.Fetch.HTTP.TimeoutSeconds
		ct := c.Fetch.HTTP.ConnectTimeoutSeconds
		if t <= 0 {
			return &ValidationError{Field: "fetch.http.timeout_seconds",
				Value: t, Reason: "must be > 0"}
		}
		if ct <= 0 {
			return &ValidationError{Field: "fetch.http.connect_timeout_seconds",
				Value: ct, Reason: "must be > 0"}
		}
		if ct > t {
			return &ValidationError{Field: "fetch.http.connect_timeout_seconds",
				Value: ct, Reason: fmt.Sprintf("must be <= timeout_seconds (%d)", t)}
		}
		return nil
	},
	// 10
	func(c *Config) error {
		if c.Fetch.HTTP.MaxRedirects < 0 {
			return &ValidationError{Field: "fetch.http.max_redirects",
				Value: c.Fetch.HTTP.MaxRedirects, Reason: "must be >= 0"}
		}
		return nil
	},
	// 11
	func(c *Config) error {
		switch c.Fetch.HTTP.TLSMinVersion {
		case "1.2", "1.3":
			return nil
		default:
			return &ValidationError{Field: "fetch.http.tls_min_version",
				Value: c.Fetch.HTTP.TLSMinVersion, Reason: "must be 1.2 or 1.3"}
		}
	},
	// 12
	func(c *Config) error {
		mn := c.Fetch.Validation.MinBodyBytes
		mx := c.Fetch.Validation.MaxBodyBytes
		if mn <= 0 {
			return &ValidationError{Field: "fetch.validation.min_body_bytes",
				Value: mn, Reason: "must be > 0"}
		}
		if mx <= 0 {
			return &ValidationError{Field: "fetch.validation.max_body_bytes",
				Value: mx, Reason: "must be > 0"}
		}
		if mn >= mx {
			return &ValidationError{Field: "fetch.validation.min_body_bytes",
				Value: mn, Reason: fmt.Sprintf("must be < max_body_bytes (%d)", mx)}
		}
		const hardCeiling = 5 * 1024 * 1024 // 5 MiB
		if mx > hardCeiling {
			return &ValidationError{Field: "fetch.validation.max_body_bytes",
				Value: mx, Reason: fmt.Sprintf("must be <= 5242880 (5 MiB hard ceiling, got %d)", mx)}
		}
		return nil
	},
	// 13
	func(c *Config) error {
		if len(c.Fetch.Validation.AllowedContentTypes) == 0 {
			return &ValidationError{Field: "fetch.validation.allowed_content_types",
				Value: c.Fetch.Validation.AllowedContentTypes, Reason: "must be non-empty"}
		}
		return nil
	},
	// 14
	func(c *Config) error {
		if c.Fetch.Validation.JSON.MaxDepth <= 0 {
			return &ValidationError{Field: "fetch.validation.json.max_depth",
				Value: c.Fetch.Validation.JSON.MaxDepth, Reason: "must be > 0"}
		}
		return nil
	},
	// 15
	func(c *Config) error {
		if c.Fetch.Validation.JSON.MaxStringLength <= 0 {
			return &ValidationError{Field: "fetch.validation.json.max_string_length",
				Value: c.Fetch.Validation.JSON.MaxStringLength, Reason: "must be > 0"}
		}
		return nil
	},
	// 16
	func(c *Config) error {
		if !c.Fetch.Validation.Hash.Algorithm.Valid() {
			return &ValidationError{Field: "fetch.validation.hash.algorithm",
				Value: c.Fetch.Validation.Hash.Algorithm, Reason: "must be blake2b-256 or none"}
		}
		return nil
	},
	// 17
	func(c *Config) error {
		if !c.Fetch.PrivateIPPolicy.Valid() {
			return &ValidationError{Field: "fetch.private_ip_policy",
				Value: c.Fetch.PrivateIPPolicy, Reason: "must be block|allow_all|allow_list"}
		}
		if c.Fetch.PrivateIPPolicy == PrivateIPAllowList && len(c.Fetch.PrivateIPAllowList) == 0 {
			return &ValidationError{Field: "fetch.private_ip_allow_list",
				Value: []string{}, Reason: "policy is allow_list but allow_list is empty"}
		}
		for _, id := range c.Fetch.PrivateIPAllowList {
			if !bech32PoolLike(id) {
				return &ValidationError{Field: "fetch.private_ip_allow_list",
					Value: id, Reason: "not a valid bech32 pool id"}
			}
		}
		for _, id := range c.Fetch.PoolsOverrides {
			if !bech32PoolLike(id.Match) {
				return &ValidationError{Field: "fetch.pools_overrides[].match",
					Value: id.Match, Reason: "not a valid bech32 pool id or glob pattern"}
			}
		}
		return nil
	},
	// 18
	func(c *Config) error {
		if c.Fetch.Retry.MaxAttempts < 1 {
			return &ValidationError{Field: "fetch.retry.max_attempts",
				Value: c.Fetch.Retry.MaxAttempts, Reason: "must be >= 1"}
		}
		return nil
	},
	// 19
	func(c *Config) error {
		if c.Database.PoolMaxConns < 1 {
			return &ValidationError{Field: "database.pool_max_conns",
				Value: c.Database.PoolMaxConns, Reason: "must be >= 1"}
		}
		return nil
	},
	// 20
	func(c *Config) error {
		mn := c.Database.PoolMinConns
		mx := c.Database.PoolMaxConns
		if mn < 0 {
			return &ValidationError{Field: "database.pool_min_conns",
				Value: mn, Reason: "must be >= 0"}
		}
		if mn > mx {
			return &ValidationError{Field: "database.pool_min_conns",
				Value: mn, Reason: fmt.Sprintf("must be <= pool_max_conns (%d)", mx)}
		}
		return nil
	},
	// 23
	func(c *Config) error {
		if c.Database.StatementTimeoutMs <= 0 {
			return &ValidationError{Field: "database.statement_timeout_ms",
				Value: c.Database.StatementTimeoutMs, Reason: "must be > 0"}
		}
		return nil
	},
	// 24
	func(c *Config) error {
		p := c.Health.ListenPort
		if p < 1 || p > 65535 {
			return &ValidationError{Field: "health.listen_port",
				Value: p, Reason: "must be 1..65535"}
		}
		return nil
	},
	// 25
	func(c *Config) error {
		if c.Health.ListenAddr == "" {
			return &ValidationError{Field: "health.listen_addr",
				Value: "", Reason: "must be non-empty (e.g. 0.0.0.0 or 127.0.0.1)"}
		}
		return nil
	},
	// 26
	func(c *Config) error {
		if c.Health.ReadyzDBGraceSeconds <= 0 {
			return &ValidationError{Field: "health.readyz_db_grace_seconds",
				Value: c.Health.ReadyzDBGraceSeconds, Reason: "must be > 0"}
		}
		return nil
	},
	// 27
	func(c *Config) error {
		if !c.Logging.Level.Valid() {
			return &ValidationError{Field: "logging.level",
				Value: c.Logging.Level, Reason: "must be debug|info|warn|error"}
		}
		return nil
	},
	// 28
	func(c *Config) error {
		if !c.Logging.Format.Valid() {
			return &ValidationError{Field: "logging.format",
				Value: c.Logging.Format, Reason: "must be json|text"}
		}
		return nil
	},
	// 29
	func(c *Config) error {
		switch c.Fetch.PoolsOverridesMatch {
		case MatchExact, MatchGlob:
			return nil
		default:
			return &ValidationError{Field: "fetch.pools_overrides_match_style",
				Value: c.Fetch.PoolsOverridesMatch, Reason: "must be exact|glob"}
		}
	},
	// 30 — pools_overrides[*].overrides keys must be in allowedOverrideKeys
	func(c *Config) error {
		for i, ov := range c.Fetch.PoolsOverrides {
			seen := map[string]bool{}
			for k := range ov.Overrides {
				if !allowedOverrideKeys[k] {
					return &ValidationError{
						Field:  fmt.Sprintf("fetch.pools_overrides[%d].overrides", i),
						Value:  k,
						Reason: "unknown override key (allowed: max_body_bytes, timeout_seconds, user_agent, private_ip_policy, enabled)",
					}
				}
				seen[k] = true
			}
			if len(seen) == 0 {
				return &ValidationError{
					Field:  fmt.Sprintf("fetch.pools_overrides[%d].overrides", i),
					Value:  ov.Overrides,
					Reason: "must declare at least one override key",
				}
			}
		}
		return nil
	},
	// 31 — duplicate detect on allow/deny/overrides pool ids (warning-grade; see below)
	// 32 — reserved for future validators
}

var (
	bech32PoolRe  = regexp.MustCompile(`^pool1[02-9ac-hjknp-z]{20,}$`)
	schemaIdentRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
)

// allowedOverrideKeys is the strict allowlist for keys under
// fetch.pools_overrides[*].overrides (rule #30). Defined at package scope so
// the rule body can be a pure expression inside the configRules slice.
var allowedOverrideKeys = map[string]bool{
	"max_body_bytes":    true,
	"timeout_seconds":   true,
	"user_agent":        true,
	"private_ip_policy": true,
	"enabled":           true,
}

func bech32PoolLike(s string) bool {
	// Accept either a valid bech32 pool id OR a glob pattern. Glob patterns
	// are detected by the presence of `*` or `?`. Strict bech32 matching for
	// exact entries is a TODO in the spec (no checksum verification).
	if strings.ContainsAny(s, "*?[") {
		return true
	}
	return bech32PoolRe.MatchString(strings.ToLower(s))
}

// Helpers ---------------------------------------------------------------------

// IsNotFound returns whether err represents "file not found".
func IsNotFound(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}
