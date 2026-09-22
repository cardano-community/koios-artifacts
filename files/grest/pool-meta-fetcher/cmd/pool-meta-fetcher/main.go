// Package main is the entry point for koios-pool-meta-fetcher.
//
// The daemon periodically fetches off-chain pool metadata from URLs declared
// in on-chain pool_update certificates, validates the responses, and upserts
// them into grest.pool_offchain_metadata.
//
// See ../../openspec/proposal.md for the change rationale and
// ../../openspec/specs/pool-offchain-fetcher/spec.md for the contract.
package main

import (
	"context"
	"crypto/tls"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/config"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/fetcher"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/health"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/ipfilter"
	"github.com/cardano-community/koios-artifacts/pool-meta-fetcher/internal/storage"
)

const (
	defaultConfigPath = "$CNODE_HOME/priv/pool-meta-fetcher.yaml"
	exitCodeConfig    = 2
	exitCodeDB        = 3
	exitCodeRuntime   = 1
)

func main() {
	cfgPath := flag.String("config", defaultConfigPath, "Path to YAML config file")
	flag.Parse()

	logger := newLoggerAt(slog.LevelInfo, config.FormatJSON, false)
	slog.SetDefault(logger)

	logger.Info("starting koios-pool-meta-fetcher",
		slog.String("config_path", *cfgPath),
		slog.String("version", version),
	)

	cfg, err := config.Load(*cfgPath, logger)
	if err != nil {
		logger.Error("config_load_failed", slog.String("err", err.Error()))
		os.Exit(exitCodeConfig)
	}
	logger.Info("config_loaded",
		slog.String("network", string(cfg.NetworkName)),
		slog.Bool("fetch_enabled", cfg.Fetch.Enabled),
		slog.Int("poll_interval_seconds", cfg.Fetch.Poll.PollIntervalSeconds),
	)

	logger = newLoggerAt(cfg.Logging.Level.ToSlog(), cfg.Logging.Format, cfg.Logging.AddSource)
	slog.SetDefault(logger)

	// Health server (always up).
	hs := health.NewServer(health.Config{
		ListenAddr:           cfg.Health.ListenAddr,
		ListenPort:           cfg.Health.ListenPort,
		ReadyzDBGraceSeconds: cfg.Health.ReadyzDBGraceSeconds,
		ExposeDebugPools:     cfg.Fetch.WorkQueue.ExposeDebugEndpoint,
	})
	if err := hs.Start(); err != nil {
		logger.Error("health_server_failed_to_start", slog.String("err", err.Error()))
		os.Exit(exitCodeRuntime)
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = hs.Stop(shutdownCtx)
	}()
	logger.Info("health_server_up", slog.String("addr", hs.Addr()))

	// Emit the spec-mandated allow/deny filter logs at startup.
	fetcher.LogFilterConfig(logger, cfg.Fetch.WorkQueue.PoolIDAllowList, cfg.Fetch.WorkQueue.PoolIDDenyList)

	// honour fetch.enabled=false as a soft disable.
	if !cfg.Fetch.Enabled {
		logger.Info("fetch_disabled_by_config")
		<-awaitShutdown()
		logger.Info("shutdown_complete")
		return
	}

	// DB pool.
	poolCtx, poolCancel := context.WithTimeout(context.Background(), 30*time.Second)
	pg, err := storage.NewPool(poolCtx, cfg.DatabaseURL, storage.PoolOptions{
		Schema:           cfg.SchemaName,
		MaxConns:         cfg.Database.PoolMaxConns,
		MinConns:         cfg.Database.PoolMinConns,
		MaxConnLifetime:  cfg.Database.PoolMaxConnLifetime,
		MaxConnIdleTime:  cfg.Database.PoolMaxConnIdleTime,
		StatementTimeout: time.Duration(cfg.Database.StatementTimeoutMs) * time.Millisecond,
		ApplicationName:  "koios-pool-meta-fetcher",
	})
	poolCancel()
	if err != nil {
		logger.Error("db_pool_init_failed", slog.String("err", err.Error()))
		os.Exit(exitCodeDB)
	}
	defer pg.Close()
	logger.Info("db_pool_ready")

	// Wire readyz.
	hs.SetPingFn(func(ctx context.Context) error {
		_, _, pingErr := pg.Ping(ctx)
		return pingErr
	})

	// Migrations FIRST (creates the schema), then adjust constraint.
	migCtx, migCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	if err := storage.RunMigrations(migCtx, cfg.DatabaseURL, cfg.SchemaName); err != nil {
		migCancel()
		logger.Error("migrations_failed", slog.String("schema", cfg.SchemaName), slog.String("err", err.Error()))
		os.Exit(exitCodeDB)
	}
	migCancel()
	logger.Info("migrations_applied", slog.String("schema", cfg.SchemaName))

	// IP filter (always instantiated — needs policy=allow_all to be a no-op).
	policy, err := ipfilter.PolicyFromString(string(cfg.Fetch.PrivateIPPolicy))
	if err != nil {
		logger.Error("ipfilter_init_failed", slog.String("err", err.Error()))
		os.Exit(exitCodeConfig)
	}
	filter := ipfilter.New(policy, cfg.Fetch.PrivateIPAllowList, logger)

	// HTTP client with IP filter + per-request timeouts.
	httpClient := &http.Client{
		Timeout: cfg.HTTPTimeout(),
		Transport: filter.WrapTransport(&http.Transport{
			TLSClientConfig: buildTLSConfig(cfg),
		}),
		CheckRedirect: buildRedirectPolicy(cfg),
	}

	// Pool fetcher.
	pf := fetcher.NewPoolFetcher(httpClient, fetcher.ValidationConfig{
		MinBodyBytes:           cfg.Fetch.Validation.MinBodyBytes,
		MaxBodyBytes:           cfg.Fetch.Validation.MaxBodyBytes,
		AllowedContentTypes:    cfg.Fetch.Validation.AllowedContentTypes,
		AllowTextHTMLWithBrace: cfg.Fetch.Validation.AllowTextHTMLWithBrace,
		JSON: fetcher.JSONValidationConfig{
			RequiredFields:        cfg.Fetch.Validation.JSON.RequiredFields,
			MaxDepth:              cfg.Fetch.Validation.JSON.MaxDepth,
			MaxStringLength:       cfg.Fetch.Validation.JSON.MaxStringLength,
			MaxKeysPerObject:      cfg.Fetch.Validation.JSON.MaxKeysPerObject,
			MaxArrayLength:        cfg.Fetch.Validation.JSON.MaxArrayLength,
			AllowAdditionalFields: cfg.Fetch.Validation.JSON.AllowAdditionalFields,
		},
		Hash: fetcher.HashValidationConfig{
			Algorithm:     string(cfg.Fetch.Validation.Hash.Algorithm),
			LogMismatches: cfg.Fetch.Validation.Hash.LogMismatches,
		},
	}, cfg.Fetch.Retry.MaxAttempts, logger)
	pf.SetUserAgent(cfg.Fetch.HTTP.UserAgent)
	pf.Errors = pg // persist failed fetches to grest.pool_offchain_fetch_error

	// Cycle driver.
	f := fetcher.New(cfg, pg, pf, logger)
	if cfg.Fetch.WorkQueue.ExposeDebugEndpoint {
		hs.SetSnapshotDebugPools(func() any { return f.Snapshot() })
	}

	// Daemon root context.
	ctx, cancel := signal.NotifyContext(context.Background(),
		syscall.SIGINT, syscall.SIGTERM,
	)
	defer cancel()

	// SIGHUP handler for live-reload. Applies only the knobs the fetcher
	// can pick up without a restart (concurrency, log level, poll interval).
	// Database URL / listen addr / port / schema name need a full restart;
	// they're reported in the reload report for the operator's awareness.
	reloadCh := make(chan os.Signal, 1)
	signal.Notify(reloadCh, syscall.SIGHUP)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range reloadCh {
			logger.Info("sighup_received")
			newCfg, err := config.Load(*cfgPath, logger)
			if err != nil {
				logger.Warn("reload_failed", slog.String("err", err.Error()))
				continue
			}
			applyReload(cfg, newCfg, hs)
			// Hand the new config to the fetcher; the loop tick picks it up.
			f.ReloadConfig(newCfg)
			logger.Info("config_reloaded")
		}
	}()

	logger.Info("entering_fetch_loop",
		slog.Int("concurrency", cfg.Fetch.WorkQueue.Concurrency),
		slog.Int("batch_size", cfg.Fetch.WorkQueue.BatchSize),
	)

	err = f.Run(ctx)
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("fetch_loop_exited_error", slog.String("err", err.Error()))
		os.Exit(exitCodeRuntime)
	}

	close(reloadCh)
	wg.Wait()
	logger.Info("shutdown_complete")
}

// -----------------------------------------------------------------------------
// TLS / redirect policy
// -----------------------------------------------------------------------------

func buildTLSConfig(cfg *config.Config) *tls.Config {
	t := &tls.Config{
		InsecureSkipVerify: cfg.Fetch.HTTP.SkipTLSVerify,
	}
	switch cfg.Fetch.HTTP.TLSMinVersion {
	case "1.3":
		t.MinVersion = tls.VersionTLS13
	default:
		t.MinVersion = tls.VersionTLS12
	}
	return t
}

// buildRedirectPolicy implements fetch.http.max_redirects and the strict
// same-host guard from spec §fetch.validation.follow_cross_host_redirects.
func buildRedirectPolicy(cfg *config.Config) func(*http.Request, []*http.Request) error {
	max := cfg.Fetch.HTTP.MaxRedirects
	followCrossHost := cfg.Fetch.Validation.FollowCrossHostRedirects
	return func(req *http.Request, via []*http.Request) error {
		if len(via) >= max {
			return fmt.Errorf("stopped after %d redirects", max)
		}
		if !followCrossHost && len(via) > 0 {
			last := via[len(via)-1].URL.Host
			this := req.URL.Host
			if last != this {
				return fmt.Errorf("cross-host redirect blocked: %s → %s", last, this)
			}
		}
		return nil
	}
}

// -----------------------------------------------------------------------------
// SIGHUP reload diffing
// -----------------------------------------------------------------------------

type reloadReport struct {
	Changed []string
	Skipped []string
}

func applyReload(old, newCfg *config.Config, hs *health.Server) reloadReport {
	r := reloadReport{}
	if old.Fetch.Poll.PollIntervalSeconds != newCfg.Fetch.Poll.PollIntervalSeconds {
		r.Changed = append(r.Changed, fmt.Sprintf("fetch.poll.poll_interval_seconds: %d → %d",
			old.Fetch.Poll.PollIntervalSeconds, newCfg.Fetch.Poll.PollIntervalSeconds))
	}
	if old.Fetch.WorkQueue.Concurrency != newCfg.Fetch.WorkQueue.Concurrency {
		r.Changed = append(r.Changed, fmt.Sprintf("fetch.work_queue.concurrency: %d → %d",
			old.Fetch.WorkQueue.Concurrency, newCfg.Fetch.WorkQueue.Concurrency))
	}
	if old.Logging.Level != newCfg.Logging.Level {
		r.Changed = append(r.Changed, fmt.Sprintf("logging.level: %s → %s",
			old.Logging.Level, newCfg.Logging.Level))
		slog.SetDefault(newLoggerAt(newCfg.Logging.Level.ToSlog(), newCfg.Logging.Format, newCfg.Logging.AddSource))
	}
	if old.Logging.Format != newCfg.Logging.Format {
		r.Changed = append(r.Changed, fmt.Sprintf("logging.format: %s → %s",
			old.Logging.Format, newCfg.Logging.Format))
		slog.SetDefault(newLoggerAt(newCfg.Logging.Level.ToSlog(), newCfg.Logging.Format, newCfg.Logging.AddSource))
	}
	if old.Logging.AddSource != newCfg.Logging.AddSource {
		r.Changed = append(r.Changed, fmt.Sprintf("logging.add_source: %v → %v",
			old.Logging.AddSource, newCfg.Logging.AddSource))
		slog.SetDefault(newLoggerAt(newCfg.Logging.Level.ToSlog(), newCfg.Logging.Format, newCfg.Logging.AddSource))
	}
	if old.Fetch.WorkQueue.ExposeDebugEndpoint != newCfg.Fetch.WorkQueue.ExposeDebugEndpoint {
		r.Changed = append(r.Changed, fmt.Sprintf("fetch.work_queue.expose_debug_endpoint: %v → %v",
			old.Fetch.WorkQueue.ExposeDebugEndpoint, newCfg.Fetch.WorkQueue.ExposeDebugEndpoint))
		if hs != nil {
			hs.SetExposeDebugPools(newCfg.Fetch.WorkQueue.ExposeDebugEndpoint)
		}
	}
	if !slicesEqual(old.Fetch.WorkQueue.PoolIDAllowList, newCfg.Fetch.WorkQueue.PoolIDAllowList) {
		r.Changed = append(r.Changed, fmt.Sprintf("fetch.work_queue.pool_id_allow_list: %v → %v",
			old.Fetch.WorkQueue.PoolIDAllowList, newCfg.Fetch.WorkQueue.PoolIDAllowList))
		fetcher.LogFilterConfig(slog.Default(), newCfg.Fetch.WorkQueue.PoolIDAllowList, newCfg.Fetch.WorkQueue.PoolIDDenyList)
	}
	if !slicesEqual(old.Fetch.WorkQueue.PoolIDDenyList, newCfg.Fetch.WorkQueue.PoolIDDenyList) {
		r.Changed = append(r.Changed, fmt.Sprintf("fetch.work_queue.pool_id_deny_list: %v → %v",
			old.Fetch.WorkQueue.PoolIDDenyList, newCfg.Fetch.WorkQueue.PoolIDDenyList))
		fetcher.LogFilterConfig(slog.Default(), newCfg.Fetch.WorkQueue.PoolIDAllowList, newCfg.Fetch.WorkQueue.PoolIDDenyList)
	}
	if old.DatabaseURL != newCfg.DatabaseURL {
		r.Skipped = append(r.Skipped, "database_url (restart required)")
	}
	if old.Health.ListenAddr != newCfg.Health.ListenAddr ||
		old.Health.ListenPort != newCfg.Health.ListenPort {
		r.Skipped = append(r.Skipped, "health.listen_addr / health.listen_port (restart required)")
	}
	if old.NetworkName != newCfg.NetworkName {
		r.Skipped = append(r.Skipped, "network_name (restart required)")
	}
	return r
}

func slicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func newLoggerAt(level slog.Level, format config.LogFormat, addSource bool) *slog.Logger {
	opts := &slog.HandlerOptions{Level: level, AddSource: addSource}
	switch format {
	case config.FormatText:
		return slog.New(slog.NewTextHandler(os.Stdout, opts))
	default:
		return slog.New(slog.NewJSONHandler(os.Stdout, opts))
	}
}

// awaitShutdown blocks until SIGINT / SIGTERM.
func awaitShutdown() <-chan struct{} {
	ch := make(chan struct{})
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		close(ch)
	}()
	return ch
}

// version is overridden at build time:
//
//	go build -ldflags "-X main.version=1.2.3"
var version = "dev"
