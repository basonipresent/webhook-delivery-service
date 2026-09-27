package dispatch

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"webhook-delivery-service/internal/store"
)

// Headers sent with every delivery attempt.
const (
	HeaderEventID   = "Webhook-Event-Id"
	HeaderTimestamp = "Webhook-Timestamp"
	HeaderSignature = "Webhook-Signature"
)

// maxResponseBytes is how much of a merchant response body is read (and
// discarded) so the connection can be reused.
const maxResponseBytes = 4 << 10

// Clock returns the current time. Injected so tests control scheduling.
type Clock interface{ Now() time.Time }

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// Config configures a Dispatcher. Use DefaultConfig and override fields.
type Config struct {
	Policy           RetryPolicy
	PerEndpointLimit int           // max in-flight attempts per endpoint
	AttemptTimeout   time.Duration // covers connect, request and response body
	PollInterval     time.Duration // how often Run looks for due deliveries
	Clock            Clock         // nil = wall clock
	Client           *http.Client  // nil = client that does not follow redirects
	Logger           *slog.Logger  // nil = slog.Default()
}

func DefaultConfig() Config {
	return Config{
		Policy:           DefaultRetryPolicy(),
		PerEndpointLimit: 4,
		AttemptTimeout:   10 * time.Second,
		PollInterval:     100 * time.Millisecond,
	}
}

// Dispatcher claims due deliveries from the store and runs each attempt in its
// own goroutine. Retries are scheduled by writing next_attempt_at back to the
// store; no goroutine waits between attempts.
type Dispatcher struct {
	store            *store.Store
	client           *http.Client
	clock            Clock
	logger           *slog.Logger
	policy           RetryPolicy
	perEndpointLimit int
	attemptTimeout   time.Duration
	pollInterval     time.Duration
	wg               sync.WaitGroup
}

func New(st *store.Store, cfg Config) (*Dispatcher, error) {
	switch {
	case st == nil:
		return nil, errors.New("new dispatcher: nil store")
	case cfg.PerEndpointLimit < 1:
		return nil, fmt.Errorf("new dispatcher: per-endpoint limit %d must be at least 1", cfg.PerEndpointLimit)
	case cfg.AttemptTimeout <= 0:
		return nil, fmt.Errorf("new dispatcher: attempt timeout %s must be positive", cfg.AttemptTimeout)
	case cfg.PollInterval <= 0:
		return nil, fmt.Errorf("new dispatcher: poll interval %s must be positive", cfg.PollInterval)
	}
	d := &Dispatcher{
		store:            st,
		client:           cfg.Client,
		clock:            cfg.Clock,
		logger:           cfg.Logger,
		policy:           cfg.Policy,
		perEndpointLimit: cfg.PerEndpointLimit,
		attemptTimeout:   cfg.AttemptTimeout,
		pollInterval:     cfg.PollInterval,
	}
	if d.client == nil {
		d.client = &http.Client{
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		}
	}
	if d.clock == nil {
		d.clock = realClock{}
	}
	if d.logger == nil {
		d.logger = slog.Default()
	}
	return d, nil
}

// Run calls DispatchDue every PollInterval until ctx is cancelled, then waits
// for in-flight attempts to finish and be recorded. It returns nil on shutdown.
func (d *Dispatcher) Run(ctx context.Context) error {
	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()
	for {
		d.DispatchDue(ctx)
		select {
		case <-ctx.Done():
			d.Wait()
			return nil
		case <-ticker.C:
		}
	}
}

// DispatchDue claims every due delivery the per-endpoint limits allow and
// starts one goroutine per attempt. It returns the number started. Attempts
// are not cancelled by ctx (they finish within AttemptTimeout); ctx only
// stops new claims.
func (d *Dispatcher) DispatchDue(ctx context.Context) int {
	if ctx.Err() != nil {
		return 0
	}
	jobs := d.store.ClaimDue(d.clock.Now(), d.perEndpointLimit)
	attemptCtx := context.WithoutCancel(ctx)
	for _, job := range jobs {
		d.wg.Go(func() { d.attempt(attemptCtx, job) })
	}
	return len(jobs)
}

// Wait blocks until all started attempts are recorded. It must not be called
// concurrently with DispatchDue.
func (d *Dispatcher) Wait() { d.wg.Wait() }

// attempt sends one request, records it and schedules what happens next.
func (d *Dispatcher) attempt(ctx context.Context, job store.Job) {
	started := d.clock.Now()
	status, retryAfter, err := d.send(ctx, job, started)
	finished := d.clock.Now()

	a := store.Attempt{
		Number:     job.AttemptNumber,
		StartedAt:  started,
		Duration:   finished.Sub(started),
		StatusCode: status,
	}
	if err != nil {
		a.Error = err.Error()
	}

	outcome := Classify(status, err)
	var errBuild *buildError
	if errors.As(err, &errBuild) {
		outcome = OutcomeFail // a request that cannot be built will never succeed
	}
	next, nextAt := store.StatusSucceeded, time.Time{}
	switch outcome {
	case OutcomeFail:
		next = store.StatusFailed
	case OutcomeRetry:
		delay, ok := d.policy.NextDelay(job.RetryIndex, retryAfter)
		if !ok {
			outcome, next = OutcomeFail, store.StatusFailed
			break
		}
		next, nextAt = store.StatusPending, finished.Add(delay)
	}
	a.Outcome = string(outcome)

	if err := d.store.Complete(job.DeliveryID, a, next, nextAt); err != nil {
		// Only possible on a bug; the delivery would stay in_flight, so make it loud.
		d.logger.Error("record delivery attempt", "delivery_id", job.DeliveryID, "attempt", job.AttemptNumber, "err", err)
	}
}

type buildError struct{ err error }

func (e *buildError) Error() string { return "build request: " + e.err.Error() }
func (e *buildError) Unwrap() error { return e.err }

// send makes the signed POST. It returns the status code (0 without a
// response), any Retry-After on the response, and the transport error. A
// failure to read the response body after the status arrived is not an error:
// the merchant has already answered.
func (d *Dispatcher) send(ctx context.Context, job store.Job, now time.Time) (int, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, d.attemptTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, job.URL, bytes.NewReader(job.Body))
	if err != nil {
		return 0, 0, &buildError{err}
	}
	ts := now.Unix()
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderEventID, job.EventID)
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderSignature, "v1="+Sign([]byte(job.Secret), ts, job.Body))

	resp, err := d.client.Do(req)
	if err != nil {
		return 0, 0, fmt.Errorf("post: %w", err)
	}
	if err := drain(resp.Body); err != nil {
		d.logger.Warn("drain response body", "delivery_id", job.DeliveryID, "status", resp.StatusCode, "err", err)
	}
	retryAfter, _ := ParseRetryAfter(resp.Header.Get("Retry-After"), now) // absent or malformed = no hint
	return resp.StatusCode, retryAfter, nil
}

func drain(body io.ReadCloser) error {
	_, readErr := io.Copy(io.Discard, io.LimitReader(body, maxResponseBytes))
	return errors.Join(readErr, body.Close())
}
