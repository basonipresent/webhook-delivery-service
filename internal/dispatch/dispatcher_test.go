package dispatch

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"webhook-delivery-service/internal/store"
)

var t0 = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

const testSecret = "0123456789abcdef"

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// merchant is an httptest server that records requests and answers with handle.
type merchant struct {
	*httptest.Server
	mu       sync.Mutex
	requests []received
}

type received struct {
	header http.Header
	body   []byte
}

func newMerchant(t *testing.T, handle http.HandlerFunc) *merchant {
	t.Helper()
	m := &merchant{}
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("merchant read body: %v", err)
		}
		m.mu.Lock()
		m.requests = append(m.requests, received{r.Header.Clone(), body})
		m.mu.Unlock()
		handle(w, r)
	}))
	t.Cleanup(m.Close)
	return m
}

func (m *merchant) received() []received {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]received(nil), m.requests...)
}

func status(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

type env struct {
	store *store.Store
	clock *fakeClock
	d     *Dispatcher
}

func newEnv(t *testing.T, mutate func(*Config)) *env {
	t.Helper()
	clock := &fakeClock{now: t0}
	cfg := DefaultConfig()
	cfg.Clock = clock
	cfg.Policy = RetryPolicy{Schedule: []time.Duration{time.Minute, 2 * time.Minute}, MaxRetryAfter: time.Hour}
	cfg.AttemptTimeout = 5 * time.Second
	cfg.PollInterval = 5 * time.Millisecond
	if mutate != nil {
		mutate(&cfg)
	}
	st := store.New()
	d, err := New(st, cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return &env{store: st, clock: clock, d: d}
}

func (e *env) endpoint(t *testing.T, url string) store.Endpoint {
	t.Helper()
	ep, err := e.store.CreateEndpoint(store.Endpoint{URL: url, Secret: testSecret, EventTypes: []string{"order.paid"}, CreatedAt: t0})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	return ep
}

// event creates an event and returns its deliveries.
func (e *env) event(t *testing.T, id string) []store.Delivery {
	t.Helper()
	ev := store.Event{ID: id, Type: "order.paid", CreatedAt: t0, Payload: json.RawMessage(`{"amount":10}`)}
	_, ds, _, err := e.store.CreateEvent(ev, e.clock.Now())
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	return ds
}

// tick runs one DispatchDue and waits for the started attempts.
func (e *env) tick(t *testing.T) int {
	t.Helper()
	n := e.d.DispatchDue(t.Context())
	e.d.Wait()
	return n
}

func (e *env) delivery(t *testing.T, id string) store.Delivery {
	t.Helper()
	d, err := e.store.GetDelivery(id)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	return d
}

func TestNewValidatesConfig(t *testing.T) {
	tests := []struct {
		name   string
		store  *store.Store
		mutate func(*Config)
	}{
		{"nil store", nil, func(*Config) {}},
		{"zero limit", store.New(), func(c *Config) { c.PerEndpointLimit = 0 }},
		{"zero attempt timeout", store.New(), func(c *Config) { c.AttemptTimeout = 0 }},
		{"negative attempt timeout", store.New(), func(c *Config) { c.AttemptTimeout = -time.Second }},
		{"zero poll interval", store.New(), func(c *Config) { c.PollInterval = 0 }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.mutate(&cfg)
			if _, err := New(tt.store, cfg); err == nil {
				t.Fatal("expected error")
			}
		})
	}
	if _, err := New(store.New(), DefaultConfig()); err != nil {
		t.Fatalf("default config: %v", err)
	}
}

func TestDeliverSuccessSignedRequest(t *testing.T) {
	e := newEnv(t, nil)
	m := newMerchant(t, status(http.StatusNoContent))
	e.endpoint(t, m.URL)
	ds := e.event(t, "evt_1")

	if n := e.tick(t); n != 1 {
		t.Fatalf("started %d attempts, want 1", n)
	}

	reqs := m.received()
	if len(reqs) != 1 {
		t.Fatalf("merchant got %d requests, want 1", len(reqs))
	}
	r := reqs[0]
	ev, _, err := e.store.GetEvent("evt_1")
	if err != nil {
		t.Fatal(err)
	}
	if string(r.body) != string(ev.Body) {
		t.Errorf("body = %s, want exact stored body %s", r.body, ev.Body)
	}
	if got := r.header.Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	if got := r.header.Get(HeaderEventID); got != "evt_1" {
		t.Errorf("%s = %q, want evt_1", HeaderEventID, got)
	}
	if got := r.header.Get(HeaderTimestamp); got != strconv.FormatInt(t0.Unix(), 10) {
		t.Errorf("%s = %q, want %d", HeaderTimestamp, got, t0.Unix())
	}
	verifySignature(t, r)

	d := e.delivery(t, ds[0].ID)
	if d.Status != store.StatusSucceeded || len(d.Attempts) != 1 {
		t.Fatalf("delivery = %+v, want succeeded with 1 attempt", d)
	}
	a := d.Attempts[0]
	if a.Number != 1 || a.StatusCode != 204 || a.Outcome != "success" || a.Error != "" || !a.StartedAt.Equal(t0) {
		t.Errorf("attempt = %+v", a)
	}
}

// verifySignature does what a merchant does: recompute the HMAC over the
// received timestamp header and raw body.
func verifySignature(t *testing.T, r received) {
	t.Helper()
	ts, err := strconv.ParseInt(r.header.Get(HeaderTimestamp), 10, 64)
	if err != nil {
		t.Fatalf("parse timestamp header: %v", err)
	}
	want := "v1=" + Sign([]byte(testSecret), ts, r.body)
	if got := r.header.Get(HeaderSignature); got != want {
		t.Fatalf("%s = %q, want %q", HeaderSignature, got, want)
	}
}

func TestRetryScheduleThenExhausted(t *testing.T) {
	e := newEnv(t, nil) // schedule: 1m, 2m
	m := newMerchant(t, status(http.StatusInternalServerError))
	e.endpoint(t, m.URL)
	id := e.event(t, "evt_1")[0].ID

	e.tick(t)
	d := e.delivery(t, id)
	if d.Status != store.StatusPending || !d.NextAttemptAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("after attempt 1: %+v, want pending at +1m", d)
	}
	if a := d.Attempts[0]; a.StatusCode != 500 || a.Outcome != "retry" {
		t.Fatalf("attempt 1 = %+v", a)
	}

	if n := e.tick(t); n != 0 {
		t.Fatalf("retried %d before due", n)
	}
	e.clock.Advance(time.Minute - time.Nanosecond)
	if n := e.tick(t); n != 0 {
		t.Fatalf("retried %d a nanosecond early", n)
	}

	e.clock.Advance(time.Nanosecond)
	if n := e.tick(t); n != 1 {
		t.Fatalf("retry not started when due")
	}
	d = e.delivery(t, id)
	if d.Status != store.StatusPending || !d.NextAttemptAt.Equal(t0.Add(3*time.Minute)) {
		t.Fatalf("after attempt 2: %+v, want pending at +3m", d)
	}

	e.clock.Advance(2 * time.Minute)
	e.tick(t)
	d = e.delivery(t, id)
	if d.Status != store.StatusFailed || len(d.Attempts) != 3 || !d.NextAttemptAt.IsZero() {
		t.Fatalf("after attempt 3: %+v, want failed with 1+len(schedule)=3 attempts", d)
	}
	if last := d.Attempts[2]; last.Outcome != "fail" || last.StatusCode != 500 {
		t.Errorf("last attempt = %+v, want fail/500", last)
	}

	e.clock.Advance(24 * time.Hour)
	if n := e.tick(t); n != 0 {
		t.Fatalf("failed delivery attempted again")
	}
	reqs := m.received()
	if len(reqs) != 3 {
		t.Fatalf("merchant got %d requests, want 3", len(reqs))
	}
	// Each attempt is freshly timestamped and signed.
	for i, r := range reqs {
		verifySignature(t, r)
		if i > 0 && r.header.Get(HeaderTimestamp) == reqs[i-1].header.Get(HeaderTimestamp) {
			t.Errorf("attempt %d reused timestamp %s", i+1, r.header.Get(HeaderTimestamp))
		}
	}
}

func TestOutcomeByResponse(t *testing.T) {
	tests := []struct {
		name       string
		handle     http.HandlerFunc
		wantStatus store.Status
		wantCode   int
		wantNextAt time.Time
	}{
		{"400 fails immediately", status(400), store.StatusFailed, 400, time.Time{}},
		{"410 fails immediately", status(410), store.StatusFailed, 410, time.Time{}},
		{"408 retries", status(408), store.StatusPending, 408, t0.Add(time.Minute)},
		{"503 retries", status(503), store.StatusPending, 503, t0.Add(time.Minute)},
		{
			"429 without Retry-After uses schedule",
			status(429), store.StatusPending, 429, t0.Add(time.Minute),
		},
		{
			"429 Retry-After seconds honored",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "600")
				w.WriteHeader(429)
			},
			store.StatusPending, 429, t0.Add(10 * time.Minute),
		},
		{
			"429 Retry-After date honored",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", t0.Add(5*time.Minute).Format(http.TimeFormat))
				w.WriteHeader(429)
			},
			store.StatusPending, 429, t0.Add(5 * time.Minute),
		},
		{
			"503 Retry-After honored",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "600")
				w.WriteHeader(503)
			},
			store.StatusPending, 503, t0.Add(10 * time.Minute),
		},
		{
			"Retry-After capped at MaxRetryAfter",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "86400")
				w.WriteHeader(429)
			},
			store.StatusPending, 429, t0.Add(time.Hour),
		},
		{
			"malformed Retry-After uses schedule",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "soon")
				w.WriteHeader(429)
			},
			store.StatusPending, 429, t0.Add(time.Minute),
		},
		{
			"Retry-After on 400 still fails",
			func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Retry-After", "60")
				w.WriteHeader(400)
			},
			store.StatusFailed, 400, time.Time{},
		},
		{
			"large 200 body succeeds",
			func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(strings.Repeat("x", 1<<20)))
			},
			store.StatusSucceeded, 200, time.Time{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, nil)
			m := newMerchant(t, tt.handle)
			e.endpoint(t, m.URL)
			id := e.event(t, "evt_1")[0].ID

			e.tick(t)
			d := e.delivery(t, id)
			if d.Status != tt.wantStatus || !d.NextAttemptAt.Equal(tt.wantNextAt) {
				t.Fatalf("delivery status %q next %v; want %q next %v", d.Status, d.NextAttemptAt, tt.wantStatus, tt.wantNextAt)
			}
			if len(d.Attempts) != 1 || d.Attempts[0].StatusCode != tt.wantCode {
				t.Fatalf("attempts = %+v, want one with status %d", d.Attempts, tt.wantCode)
			}
		})
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	e := newEnv(t, nil)
	target := newMerchant(t, status(200))
	m := newMerchant(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	})
	e.endpoint(t, m.URL)
	id := e.event(t, "evt_1")[0].ID

	e.tick(t)
	d := e.delivery(t, id)
	if d.Status != store.StatusFailed || d.Attempts[0].StatusCode != 302 {
		t.Fatalf("delivery = %+v, want failed with 302", d)
	}
	if n := len(target.received()); n != 0 {
		t.Fatalf("redirect target got %d requests, want 0", n)
	}
}

func TestTransportErrorsRetry(t *testing.T) {
	release := make(chan struct{})
	hanging := newMerchant(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(release) }) // runs before hanging.Close (LIFO)

	closed := httptest.NewServer(http.NotFoundHandler())
	closedURL := closed.URL
	closed.Close()

	tests := []struct {
		name string
		url  string
	}{
		{"timeout", hanging.URL},
		{"connection refused", closedURL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newEnv(t, func(c *Config) { c.AttemptTimeout = 100 * time.Millisecond })
			e.endpoint(t, tt.url)
			id := e.event(t, "evt_1")[0].ID

			e.tick(t)
			d := e.delivery(t, id)
			if d.Status != store.StatusPending || !d.NextAttemptAt.Equal(t0.Add(time.Minute)) {
				t.Fatalf("delivery = %+v, want pending at +1m", d)
			}
			a := d.Attempts[0]
			if a.StatusCode != 0 || a.Error == "" || a.Outcome != "retry" {
				t.Fatalf("attempt = %+v, want status 0 with error and retry", a)
			}
		})
	}
}

func TestUnbuildableRequestFails(t *testing.T) {
	e := newEnv(t, nil)
	e.endpoint(t, "http://bad host/") // the API rejects this; the dispatcher must not retry it for 24h
	id := e.event(t, "evt_1")[0].ID

	e.tick(t)
	d := e.delivery(t, id)
	if d.Status != store.StatusFailed || !strings.Contains(d.Attempts[0].Error, "build request") {
		t.Fatalf("delivery = %+v, want failed with build error", d)
	}
}

// TestSlowEndpointDoesNotBlockOthers: endpoint A hangs on every request; B is
// fast. All of B's deliveries must succeed while A holds its capped slots.
func TestSlowEndpointDoesNotBlockOthers(t *testing.T) {
	const events = 20
	release := make(chan struct{})
	slow := newMerchant(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	fast := newMerchant(t, status(200))
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock) // unblock A before servers close

	e := newEnv(t, func(c *Config) { c.AttemptTimeout = time.Minute })
	slowEP := e.endpoint(t, slow.URL)
	fastEP := e.endpoint(t, fast.URL)
	for i := range events {
		e.event(t, fmt.Sprintf("evt_%d", i))
	}

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.d.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(fast.received()) < events && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(fast.received()); n != events {
		t.Fatalf("fast endpoint got %d/%d requests while slow endpoint hung", n, events)
	}
	// B's deliveries are recorded shortly after the responses arrive.
	for time.Now().Before(deadline) && countStatus(t, e, fastEP.ID, store.StatusSucceeded) < events {
		time.Sleep(5 * time.Millisecond)
	}
	if n := countStatus(t, e, fastEP.ID, store.StatusSucceeded); n != events {
		t.Fatalf("fast endpoint: %d/%d succeeded", n, events)
	}
	if n := len(slow.received()); n != DefaultConfig().PerEndpointLimit {
		t.Errorf("slow endpoint got %d concurrent requests, want cap %d", n, DefaultConfig().PerEndpointLimit)
	}
	if n := countStatus(t, e, slowEP.ID, store.StatusInFlight); n != DefaultConfig().PerEndpointLimit {
		t.Errorf("slow endpoint in flight = %d, want %d", n, DefaultConfig().PerEndpointLimit)
	}

	unblock()
	cancel()
	if err := <-done; err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func countStatus(t *testing.T, e *env, endpointID string, want store.Status) int {
	t.Helper()
	n := 0
	for i := range 20 {
		_, ds, err := e.store.GetEvent(fmt.Sprintf("evt_%d", i))
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range ds {
			if d.EndpointID == endpointID && d.Status == want {
				n++
			}
		}
	}
	return n
}

// TestRunWaitsForInFlightOnCancel: after cancel, Run starts no new attempts
// but returns only once the in-flight attempt has been recorded.
func TestRunWaitsForInFlightOnCancel(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	m := newMerchant(t, func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
	})

	e := newEnv(t, func(c *Config) { c.PerEndpointLimit = 1 })
	e.endpoint(t, m.URL)
	first := e.event(t, "evt_1")[0].ID
	e.clock.Advance(time.Nanosecond) // evt_1 is strictly older, so it is claimed first
	second := e.event(t, "evt_2")[0].ID

	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- e.d.Run(ctx) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("attempt never reached the merchant")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("Run returned (%v) while an attempt was in flight", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after in-flight attempt finished")
	}

	if d := e.delivery(t, first); d.Status != store.StatusSucceeded {
		t.Errorf("in-flight delivery = %q, want succeeded (recorded before Run returned)", d.Status)
	}
	if d := e.delivery(t, second); d.Status != store.StatusPending || len(d.Attempts) != 0 {
		t.Errorf("second delivery = %+v, want untouched pending after shutdown", d)
	}
}

func TestDispatchDueAfterCancelStartsNothing(t *testing.T) {
	e := newEnv(t, nil)
	m := newMerchant(t, status(200))
	e.endpoint(t, m.URL)
	e.event(t, "evt_1")

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if n := e.d.DispatchDue(ctx); n != 0 {
		t.Fatalf("started %d after cancel", n)
	}
	e.d.Wait()
	if len(m.received()) != 0 {
		t.Fatal("merchant contacted after cancel")
	}
}

// TestRedeliverGetsFullRetryBudget: a delivery that exhausted its schedule is
// picked up on the next tick after Redeliver and retried on the full schedule again.
func TestRedeliverGetsFullRetryBudget(t *testing.T) {
	e := newEnv(t, nil) // schedule: 1m, 2m -> 3 attempts per cycle
	m := newMerchant(t, status(http.StatusServiceUnavailable))
	e.endpoint(t, m.URL)
	id := e.event(t, "evt_1")[0].ID

	runCycle := func() {
		t.Helper()
		e.tick(t)
		e.clock.Advance(time.Minute)
		e.tick(t)
		e.clock.Advance(2 * time.Minute)
		e.tick(t)
	}
	runCycle()
	if d := e.delivery(t, id); d.Status != store.StatusFailed || len(d.Attempts) != 3 {
		t.Fatalf("after first cycle: %+v, want failed with 3 attempts", d)
	}

	e.clock.Advance(time.Hour)
	if _, err := e.store.Redeliver(id, e.clock.Now()); err != nil {
		t.Fatal(err)
	}
	redeliveredAt := e.clock.Now()
	if n := e.tick(t); n != 1 {
		t.Fatalf("redelivered delivery not picked up on next tick (started %d)", n)
	}
	d := e.delivery(t, id)
	if d.Status != store.StatusPending || !d.NextAttemptAt.Equal(redeliveredAt.Add(time.Minute)) {
		t.Fatalf("after redeliver attempt: %+v, want pending at +1m (schedule restarted)", d)
	}

	e.clock.Advance(time.Minute)
	e.tick(t)
	e.clock.Advance(2 * time.Minute)
	e.tick(t)
	d = e.delivery(t, id)
	if d.Status != store.StatusFailed || len(d.Attempts) != 6 {
		t.Fatalf("after second cycle: %+v, want failed with 6 attempts", d)
	}
	for i, a := range d.Attempts {
		if a.Number != i+1 {
			t.Errorf("attempt %d numbered %d", i+1, a.Number)
		}
	}
	if n := len(m.received()); n != 6 {
		t.Errorf("merchant got %d requests, want 6", n)
	}
}
