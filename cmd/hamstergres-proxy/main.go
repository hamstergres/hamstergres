// Command hamstergres-proxy runs the Hamstergres PostgreSQL gateway.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jruszo/hamstergres/internal/backend"
	"github.com/jruszo/hamstergres/internal/config"
	"github.com/jruszo/hamstergres/internal/observability"
	"github.com/jruszo/hamstergres/internal/proxy"
	"github.com/jruszo/hamstergres/internal/status"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "status" {
		statusCommand(os.Args[2:])
		return
	}
	serveCommand(os.Args[1:])
}

func serveCommand(args []string) {
	flags := flag.NewFlagSet("hamstergres-proxy", flag.ExitOnError)
	configPath := flags.String("config", "config/hamstergres.example.yaml", "path to gateway YAML configuration")
	flags.Parse(args)

	cfg, err := config.Load(*configPath)
	if err != nil {
		slog.Error("load configuration", "error", err)
		os.Exit(1)
	}
	closeLog, err := configureLogging(cfg.Observability.LogFile)
	if err != nil {
		slog.Error("configure local log file", "event", "logging_configuration_failed", "component", "hamstergres-proxy", "error_category", "configuration", "error", err)
		os.Exit(1)
	}
	defer closeLog()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	shutdownTracing, err := observability.ConfigureTracing(ctx)
	if err != nil {
		slog.Warn("configure tracing exporter", "event", "tracing_configuration_failed", "error_category", "observability", "error", err)
	} else {
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := shutdownTracing(shutdownCtx); err != nil {
				slog.Warn("shutdown tracing exporter", "event", "tracing_shutdown_failed", "error_category", "observability", "error", err)
			}
		}()
	}
	backends, err := backend.New(ctx, cfg)
	if err != nil {
		slog.Error("initialize backend pools", "error", err)
		os.Exit(1)
	}
	defer backends.Close()

	frontend := proxy.New(backends, slog.Default())
	statusServer := status.New(backends, frontend)
	httpServer := &http.Server{Addr: cfg.Status.Address, Handler: statusServer.Handler(), ReadHeaderTimeout: 5 * time.Second}
	listener, err := net.Listen("tcp", cfg.Listen.Address)
	if err != nil {
		slog.Error("listen for PostgreSQL clients", "address", cfg.Listen.Address, "error", err)
		os.Exit(1)
	}
	defer listener.Close()

	go func() {
		slog.Info("status server listening", "address", cfg.Status.Address)
		if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("status server stopped", "error", err)
			stop()
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = httpServer.Shutdown(shutdownCtx)
		_ = listener.Close()
	}()

	slog.Info("PostgreSQL gateway listening", "address", cfg.Listen.Address, "burrows", cfg.ShardNames())
	if err := frontend.Serve(listener); err != nil && !errors.Is(err, net.ErrClosed) {
		slog.Error("PostgreSQL gateway stopped", "error", err)
		os.Exit(1)
	}
}

func configureLogging(path string) (func(), error) {
	if path == "" {
		return func() {}, nil
	}
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	logger := slog.New(slog.NewJSONHandler(file, &slog.HandlerOptions{Level: slog.LevelInfo})).With("component", "hamstergres-proxy")
	slog.SetDefault(logger)
	return func() { _ = file.Close() }, nil
}

func statusCommand(args []string) {
	flags := flag.NewFlagSet("hamstergres-proxy status", flag.ExitOnError)
	statusURL := flags.String("status-url", "http://127.0.0.1:8080/api/v1/status", "gateway status endpoint")
	flags.Parse(args)

	response, err := http.Get(*statusURL) // #nosec G107 -- the operator explicitly supplies the local control endpoint.
	if err != nil {
		slog.Error("request gateway status", "error", err)
		os.Exit(1)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		slog.Error("gateway status request failed", "status", response.Status)
		os.Exit(1)
	}
	var snapshot status.Snapshot
	if err := json.NewDecoder(response.Body).Decode(&snapshot); err != nil {
		slog.Error("decode gateway status", "error", err)
		os.Exit(1)
	}
	fmt.Printf("Hamstergres status (%s)\n", snapshot.Now.Format(time.RFC3339))
	fmt.Printf("Uptime: %ds\n", snapshot.UptimeSeconds)
	fmt.Printf("Frontend: %d active / %d total connections\n", snapshot.Frontend.ActiveConnections, snapshot.Frontend.Connections)
	fmt.Printf("Queries: %d total / %d failed (average %dms)\n", snapshot.Queries.Queries, snapshot.Queries.FailedQueries, snapshot.Queries.AverageDurationMillis)
	fmt.Printf("Routing: %d scattered / %d single-shard\n", snapshot.QueryMetrics.Total.ScatteredQueries, snapshot.QueryMetrics.Total.SingleShardQueries)
	fmt.Println("Rolling query traffic:")
	for _, window := range snapshot.QueryMetrics.Windows {
		fmt.Printf("  %s: %d queries, %d failed, %d scattered, %d single-shard, %dms average\n", window.Name, window.Statistics.Queries, window.Statistics.FailedQueries, window.Statistics.ScatteredQueries, window.Statistics.SingleShardQueries, window.Statistics.AverageDurationMillis)
		for _, shard := range window.ShardExecutions {
			fmt.Printf("    %s: %d executions\n", shard.Name, shard.Queries)
		}
	}
	if len(snapshot.QueryMetrics.QuerySummaries) > 0 {
		fmt.Println("Query summaries:")
		for _, summary := range snapshot.QueryMetrics.QuerySummaries {
			fmt.Printf("  %s [%s] (%s): %d queries, %d failures, %d scattered\n", summary.QueryShape, summary.Fingerprint, summary.Statement, summary.Statistics.Queries, summary.Statistics.FailedQueries, summary.Statistics.ScatteredQueries)
		}
	}
	for _, burrow := range snapshot.Burrows {
		health := "healthy"
		if !burrow.Healthy {
			health = "unhealthy: " + burrow.LastError
		}
		fmt.Printf("%s: %s (%d acquired, %d idle, %d total connections)\n", burrow.Name, health, burrow.AcquiredConns, burrow.IdleConns, burrow.TotalConns)
	}
}
