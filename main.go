// Command webhook-delivery-service accepts events over HTTP and delivers them
// to subscribed merchant endpoints with signed, retried requests.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"webhook-delivery-service/internal/api"
	"webhook-delivery-service/internal/dispatch"
	"webhook-delivery-service/internal/store"
)

// shutdownTimeout bounds how long in-flight HTTP requests may take to finish
// on shutdown. In-flight deliveries are bounded separately by AttemptTimeout.
const shutdownTimeout = 10 * time.Second

type config struct {
	addr     string
	dispatch dispatch.Config
}

// loadConfig reads PORT (default 8080) and RETRY_SCHEDULE (comma-separated
// durations, default ~24h exponential) from getenv.
func loadConfig(getenv func(string) string) (config, error) {
	cfg := config{addr: ":8080", dispatch: dispatch.DefaultConfig()}

	if p := getenv("PORT"); p != "" {
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return config{}, fmt.Errorf("PORT %q: must be a number between 1 and 65535", p)
		}
		cfg.addr = ":" + p
	}
	if s := getenv("RETRY_SCHEDULE"); s != "" {
		schedule, err := dispatch.ParseSchedule(s)
		if err != nil {
			return config{}, fmt.Errorf("RETRY_SCHEDULE: %w", err)
		}
		cfg.dispatch.Policy.Schedule = schedule
	}
	return cfg, nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger); err != nil {
		logger.Error("service stopped", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.addr, err)
	}
	logger.Info("listening", "addr", ln.Addr().String(), "retry_schedule", fmt.Sprint(cfg.dispatch.Policy.Schedule))
	return serve(ctx, ln, cfg, logger)
}

// serve runs the HTTP API and the dispatcher until ctx is cancelled. Shutdown
// order: stop accepting events, finish in-flight HTTP requests, then stop the
// dispatcher and wait for in-flight delivery attempts to be recorded.
func serve(ctx context.Context, ln net.Listener, cfg config, logger *slog.Logger) error {
	st := store.New()
	cfg.dispatch.Logger = logger
	d, err := dispatch.New(st, cfg.dispatch)
	if err != nil {
		return fmt.Errorf("create dispatcher: %w", err)
	}
	srv := &http.Server{
		Handler:           api.New(st, nil, logger),
		ReadHeaderTimeout: 5 * time.Second,
	}

	dispatchCtx, stopDispatch := context.WithCancel(context.WithoutCancel(ctx))
	defer stopDispatch()
	dispatchDone := make(chan error, 1)
	go func() { dispatchDone <- d.Run(dispatchCtx) }()

	serveDone := make(chan error, 1)
	go func() { serveDone <- srv.Serve(ln) }()

	select {
	case err := <-serveDone:
		stopDispatch()
		return errors.Join(fmt.Errorf("serve http: %w", err), wrap("run dispatcher", <-dispatchDone))
	case <-ctx.Done():
	}

	logger.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()
	shutdownErr := srv.Shutdown(shutdownCtx)
	if err := <-serveDone; !errors.Is(err, http.ErrServerClosed) {
		shutdownErr = errors.Join(shutdownErr, err)
	}
	stopDispatch()
	return errors.Join(wrap("shutdown http", shutdownErr), wrap("run dispatcher", <-dispatchDone))
}

func wrap(msg string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", msg, err)
}
