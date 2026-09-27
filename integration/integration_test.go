//go:build integration

// Package integration runs black-box tests against a running service.
//
// Environment:
//
//	BASE_URL       service URL (default http://localhost:8080)
//	MERCHANT_HOST  host the service uses to reach test merchants
//	               (default host.docker.internal, for the service in Docker)
//	MERCHANT_BIND  interface test merchants listen on (default 0.0.0.0)
package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

const secret = "integration-secret-0123456789"

var (
	baseURL      = envOr("BASE_URL", "http://localhost:8080")
	merchantHost = envOr("MERCHANT_HOST", "host.docker.internal")
	merchantBind = envOr("MERCHANT_BIND", "0.0.0.0")
	client       = &http.Client{Timeout: 15 * time.Second}
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func TestMain(m *testing.M) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	err := waitForService(ctx)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "service at %s not ready: %v\n", baseURL, err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func waitForService(ctx context.Context) error {
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+"/healthz", nil)
		if err != nil {
			return fmt.Errorf("build request: %w", err)
		}
		resp, err := client.Do(req)
		if err == nil {
			resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("gave up polling /healthz (last error: %v): %w", err, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// ---------- HTTP helpers ----------

type response struct {
	code int
	raw  []byte
	obj  map[string]any
}

func call(t *testing.T, ctx context.Context, method, path string, body any) response {
	t.Helper()
	r, err := doCall(ctx, method, path, body)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	return r
}

// doCall is safe to use from goroutines: it returns errors instead of failing.
func doCall(ctx context.Context, method, path string, body any) (response, error) {
	var rd io.Reader
	switch b := body.(type) {
	case nil:
	case []byte:
		rd = bytes.NewReader(b)
	default:
		enc, err := json.Marshal(b)
		if err != nil {
			return response{}, fmt.Errorf("marshal: %w", err)
		}
		rd = bytes.NewReader(enc)
	}
	req, err := http.NewRequestWithContext(ctx, method, baseURL+path, rd)
	if err != nil {
		return response{}, fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return response{}, fmt.Errorf("do: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return response{}, fmt.Errorf("read body: %w", err)
	}
	r := response{code: resp.StatusCode, raw: raw}
	if len(raw) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(raw, &r.obj); err != nil {
			return response{}, fmt.Errorf("decode %q: %w", raw, err)
		}
	}
	return r, nil
}

func expectCode(t *testing.T, r response, want int) {
	t.Helper()
	if r.code != want {
		t.Fatalf("status = %d, want %d; body: %s", r.code, want, r.raw)
	}
}

func assertFields(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	got := make([]string, 0, len(m))
	for k := range m {
		got = append(got, k)
	}
	slices.Sort(got)
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("%s fields = %v, want %v", what, got, want)
	}
}

var (
	endpointFields    = []string{"id", "url", "event_types", "created_at"}
	createEventFields = []string{"event_id", "deliveries"}
	summaryFields     = []string{"id", "endpoint_id", "status"}
	eventFields       = []string{"event_id", "type", "created_at", "received_at", "payload", "deliveries"}
	deliveryFields    = []string{"id", "event_id", "endpoint_id", "status", "next_attempt_at", "attempts"}
	attemptFields     = []string{"number", "started_at", "duration_ms", "status_code", "error", "outcome"}
)

// unique returns an identifier that differs across scenarios and runs.
func unique(scenario string) string {
	return fmt.Sprintf("%s_%d", scenario, time.Now().UnixNano())
}

func registerEndpoint(t *testing.T, ctx context.Context, url, eventType string) string {
	t.Helper()
	r := call(t, ctx, http.MethodPost, "/endpoints", map[string]any{
		"url": url, "event_types": []string{eventType}, "secret": secret,
	})
	expectCode(t, r, http.StatusCreated)
	return r.obj["id"].(string)
}

func eventBody(id, typ string) map[string]any {
	return map[string]any{
		"event_id":   id,
		"type":       typ,
		"created_at": "2026-09-27T12:00:00Z",
		"payload":    map[string]any{"scenario": id, "amount": 42},
	}
}

// postEvent creates an event and returns delivery IDs keyed by endpoint ID.
func postEvent(t *testing.T, ctx context.Context, id, typ string) map[string]string {
	t.Helper()
	r := call(t, ctx, http.MethodPost, "/events", eventBody(id, typ))
	expectCode(t, r, http.StatusAccepted)
	out := map[string]string{}
	for _, d := range r.obj["deliveries"].([]any) {
		dm := d.(map[string]any)
		out[dm["endpoint_id"].(string)] = dm["id"].(string)
	}
	return out
}

type delivery struct {
	ID            string     `json:"id"`
	EventID       string     `json:"event_id"`
	EndpointID    string     `json:"endpoint_id"`
	Status        string     `json:"status"`
	NextAttemptAt *time.Time `json:"next_attempt_at"`
	Attempts      []attempt  `json:"attempts"`
}

type attempt struct {
	Number     int       `json:"number"`
	StartedAt  time.Time `json:"started_at"`
	DurationMS int64     `json:"duration_ms"`
	StatusCode int       `json:"status_code"`
	Error      string    `json:"error"`
	Outcome    string    `json:"outcome"`
}

func getDelivery(t *testing.T, ctx context.Context, id string) delivery {
	t.Helper()
	r := call(t, ctx, http.MethodGet, "/deliveries/"+id, nil)
	expectCode(t, r, http.StatusOK)
	var d delivery
	if err := json.Unmarshal(r.raw, &d); err != nil {
		t.Fatalf("decode delivery: %v", err)
	}
	return d
}

// waitFor polls cond until it returns true or timeout elapses.
func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func waitForStatus(t *testing.T, ctx context.Context, id, status string, timeout time.Duration) delivery {
	t.Helper()
	var d delivery
	waitFor(t, timeout, "delivery "+id+" to be "+status, func() bool {
		d = getDelivery(t, ctx, id)
		return d.Status == status
	})
	return d
}

// ---------- test merchant ----------

type request struct {
	header http.Header
	body   []byte
}

// merchant is a local HTTP server the service delivers to. Its behaviour can
// be switched while the test runs.
type merchant struct {
	srv *httptest.Server
	url string // as seen from the service

	mu          sync.Mutex
	handler     http.HandlerFunc
	requests    []request
	inFlight    int
	maxInFlight int
}

func newMerchant(t *testing.T, h http.HandlerFunc) *merchant {
	t.Helper()
	ln, err := net.Listen("tcp", net.JoinHostPort(merchantBind, "0"))
	if err != nil {
		t.Fatalf("merchant listen: %v", err)
	}
	m := &merchant{handler: h}
	m.srv = httptest.NewUnstartedServer(http.HandlerFunc(m.serve))
	if err := m.srv.Listener.Close(); err != nil {
		t.Fatalf("close default listener: %v", err)
	}
	m.srv.Listener = ln
	m.srv.Start()
	t.Cleanup(m.srv.Close)
	port := ln.Addr().(*net.TCPAddr).Port
	m.url = fmt.Sprintf("http://%s/hook", net.JoinHostPort(merchantHost, strconv.Itoa(port)))
	return m
}

func (m *merchant) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body) // a failed read shows up as a signature mismatch
	m.mu.Lock()
	m.requests = append(m.requests, request{r.Header.Clone(), body})
	m.inFlight++
	m.maxInFlight = max(m.maxInFlight, m.inFlight)
	h := m.handler
	m.mu.Unlock()

	h(w, r)

	m.mu.Lock()
	m.inFlight--
	m.mu.Unlock()
}

func (m *merchant) setHandler(h http.HandlerFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.handler = h
}

func (m *merchant) received() []request {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.requests)
}

func (m *merchant) peakInFlight() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.maxInFlight
}

func respond(code int) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }
}

// hang blocks until release is closed or the service gives up on the request.
func hang(release <-chan struct{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}
}

func verifySignature(t *testing.T, r request) {
	t.Helper()
	ts := r.header.Get("Webhook-Timestamp")
	if _, err := strconv.ParseInt(ts, 10, 64); err != nil {
		t.Fatalf("Webhook-Timestamp %q is not unix seconds: %v", ts, err)
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(ts + "."))
	mac.Write(r.body)
	want := "v1=" + hex.EncodeToString(mac.Sum(nil))
	if got := r.header.Get("Webhook-Signature"); got != want {
		t.Fatalf("Webhook-Signature = %q, want %q", got, want)
	}
}

// ---------- tests ----------

func TestRegisterEndpoint(t *testing.T) {
	ctx := t.Context()
	r := call(t, ctx, http.MethodPost, "/endpoints", map[string]any{
		"url": "https://merchant.example/hook", "event_types": []string{"a", "b"}, "secret": secret,
	})
	expectCode(t, r, http.StatusCreated)
	assertFields(t, "endpoint", r.obj, endpointFields...)
	if strings.Contains(string(r.raw), secret) {
		t.Errorf("response leaks the secret: %s", r.raw)
	}

	bad := call(t, ctx, http.MethodPost, "/endpoints", map[string]any{
		"url": "ftp://merchant.example", "event_types": []string{"a"}, "secret": secret,
	})
	expectCode(t, bad, http.StatusBadRequest)
	assertFields(t, "error", bad.obj, "error")
}

// TestDeliverySignedAndRecorded: the happy path through every read endpoint,
// plus what a merchant receives (exact body, event_id, valid signature).
func TestDeliverySignedAndRecorded(t *testing.T) {
	ctx := t.Context()
	typ := unique("happy")
	m := newMerchant(t, respond(http.StatusOK))
	epID := registerEndpoint(t, ctx, m.url, typ)

	evID := unique("evt_happy")
	r := call(t, ctx, http.MethodPost, "/events", eventBody(evID, typ))
	expectCode(t, r, http.StatusAccepted)
	assertFields(t, "create event", r.obj, createEventFields...)
	ds := r.obj["deliveries"].([]any)
	if len(ds) != 1 {
		t.Fatalf("deliveries = %v, want exactly one (unique event type)", ds)
	}
	summary := ds[0].(map[string]any)
	assertFields(t, "delivery summary", summary, summaryFields...)
	if summary["endpoint_id"] != epID {
		t.Fatalf("delivery to %v, want %s", summary["endpoint_id"], epID)
	}
	dlvID := summary["id"].(string)

	d := waitForStatus(t, ctx, dlvID, "succeeded", 10*time.Second)
	if len(d.Attempts) != 1 || d.Attempts[0].StatusCode != 200 || d.Attempts[0].Outcome != "success" || d.NextAttemptAt != nil {
		t.Fatalf("delivery = %+v, want one successful attempt and null next_attempt_at", d)
	}

	reqs := m.received()
	if len(reqs) < 1 {
		t.Fatal("merchant received nothing")
	}
	got := reqs[0]
	verifySignature(t, got)
	if ct := got.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if h := got.header.Get("Webhook-Event-Id"); h != evID {
		t.Errorf("Webhook-Event-Id = %q, want %q", h, evID)
	}
	var envelope map[string]any
	if err := json.Unmarshal(got.body, &envelope); err != nil {
		t.Fatalf("merchant body is not JSON: %v", err)
	}
	assertFields(t, "merchant body", envelope, "event_id", "type", "created_at", "payload")
	if envelope["event_id"] != evID || envelope["type"] != typ {
		t.Errorf("merchant body = %v", envelope)
	}
	if p := envelope["payload"].(map[string]any); p["amount"] != float64(42) || p["scenario"] != evID {
		t.Errorf("payload = %v", p)
	}

	ev := call(t, ctx, http.MethodGet, "/events/"+evID, nil)
	expectCode(t, ev, http.StatusOK)
	assertFields(t, "event", ev.obj, eventFields...)
	full := ev.obj["deliveries"].([]any)[0].(map[string]any)
	assertFields(t, "delivery", full, deliveryFields...)
	assertFields(t, "attempt", full["attempts"].([]any)[0].(map[string]any), attemptFields...)

	dr := call(t, ctx, http.MethodGet, "/deliveries/"+dlvID, nil)
	assertFields(t, "delivery", dr.obj, deliveryFields...)
}

func TestEventWithoutSubscribers(t *testing.T) {
	r := call(t, t.Context(), http.MethodPost, "/events", eventBody(unique("evt_nosub"), unique("nobody")))
	expectCode(t, r, http.StatusAccepted)
	if ds := r.obj["deliveries"].([]any); len(ds) != 0 {
		t.Fatalf("deliveries = %v, want []", ds)
	}
}

func TestEventErrors(t *testing.T) {
	ctx := t.Context()

	missing := call(t, ctx, http.MethodPost, "/events", map[string]any{"event_id": unique("evt_bad"), "type": "x"})
	expectCode(t, missing, http.StatusBadRequest)
	assertFields(t, "error", missing.obj, "error")

	badID := call(t, ctx, http.MethodPost, "/events", eventBody("has space "+unique("x"), "x"))
	expectCode(t, badID, http.StatusBadRequest)

	huge := []byte(`{"event_id":"` + unique("evt_huge") + `","type":"x","created_at":"2026-09-27T12:00:00Z","payload":"` +
		strings.Repeat("x", 1<<20) + `"}`)
	tooBig := call(t, ctx, http.MethodPost, "/events", huge)
	expectCode(t, tooBig, http.StatusRequestEntityTooLarge)

	for _, path := range []string{"/events/" + unique("nope"), "/deliveries/" + unique("nope")} {
		nf := call(t, ctx, http.MethodGet, path, nil)
		expectCode(t, nf, http.StatusNotFound)
		assertFields(t, "error", nf.obj, "error")
	}
}

// TestDuplicateEventID: identical replays are idempotent (200, same
// deliveries, never re-fanned out), including 20 concurrent replays; different
// content for the same ID is a 409.
func TestDuplicateEventID(t *testing.T) {
	ctx := t.Context()
	typ := unique("dup")
	m := newMerchant(t, respond(http.StatusOK))
	registerEndpoint(t, ctx, m.url, typ)
	evID := unique("evt_dup")
	body := eventBody(evID, typ)

	const n = 20
	type result struct {
		code int
		ids  []string
		err  error
	}
	results := make(chan result, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			r, err := doCall(ctx, http.MethodPost, "/events", body)
			if err != nil {
				results <- result{err: err}
				return
			}
			var ids []string
			for _, d := range r.obj["deliveries"].([]any) {
				ids = append(ids, d.(map[string]any)["id"].(string))
			}
			results <- result{code: r.code, ids: ids}
		})
	}
	wg.Wait()
	close(results)

	counts := map[int]int{}
	var firstIDs []string
	for r := range results {
		if r.err != nil {
			t.Errorf("post: %v", r.err)
			continue
		}
		counts[r.code]++
		if firstIDs == nil {
			firstIDs = r.ids
		}
		if !slices.Equal(r.ids, firstIDs) || len(r.ids) != 1 {
			t.Errorf("replay returned deliveries %v, want the same single delivery %v", r.ids, firstIDs)
		}
	}
	if counts[http.StatusAccepted] != 1 || counts[http.StatusOK] != n-1 {
		t.Fatalf("status counts = %v, want one 202 and %d 200s", counts, n-1)
	}

	conflict := body
	conflict["payload"] = map[string]any{"different": true}
	r := call(t, ctx, http.MethodPost, "/events", conflict)
	expectCode(t, r, http.StatusConflict)
	assertFields(t, "error", r.obj, "error")

	ev := call(t, ctx, http.MethodGet, "/events/"+evID, nil)
	if ds := ev.obj["deliveries"].([]any); len(ds) != 1 {
		t.Fatalf("event has %d deliveries after replays, want 1", len(ds))
	}
}

// TestSlowEndpointDoesNotBlockOthers sends 20 events in parallel to a hanging
// endpoint and a fast one. Every fast delivery succeeds while the slow
// endpoint is stuck, the slow endpoint never has more than 4 requests in
// flight, and slow attempts are cut off by the 10s timeout and retried.
func TestSlowEndpointDoesNotBlockOthers(t *testing.T) {
	ctx := t.Context()
	typ := unique("isolation")
	release := make(chan struct{})
	slow := newMerchant(t, hang(release))
	t.Cleanup(sync.OnceFunc(func() { close(release) })) // before merchant Close (LIFO)
	fast := newMerchant(t, respond(http.StatusOK))
	slowID := registerEndpoint(t, ctx, slow.url, typ)
	fastID := registerEndpoint(t, ctx, fast.url, typ)

	const n = 20
	type result struct {
		deliveries map[string]string
		err        error
	}
	results := make(chan result, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Go(func() {
			r, err := doCall(ctx, http.MethodPost, "/events", eventBody(unique(fmt.Sprintf("evt_iso%d", i)), typ))
			if err == nil && r.code != http.StatusAccepted {
				err = fmt.Errorf("status %d: %s", r.code, r.raw)
			}
			if err != nil {
				results <- result{err: err}
				return
			}
			ds := map[string]string{}
			for _, d := range r.obj["deliveries"].([]any) {
				dm := d.(map[string]any)
				ds[dm["endpoint_id"].(string)] = dm["id"].(string)
			}
			results <- result{deliveries: ds}
		})
	}
	wg.Wait()
	close(results)
	var fastIDs, slowIDs []string
	for r := range results {
		if r.err != nil {
			t.Errorf("post event: %v", r.err)
			continue
		}
		fastIDs = append(fastIDs, r.deliveries[fastID])
		slowIDs = append(slowIDs, r.deliveries[slowID])
	}
	if t.Failed() {
		t.FailNow()
	}

	// Well inside the 10s attempt timeout: the slow endpoint is still hanging.
	start := time.Now()
	waitFor(t, 5*time.Second, "all fast deliveries to succeed", func() bool {
		for _, id := range fastIDs {
			if getDelivery(t, ctx, id).Status != "succeeded" {
				return false
			}
		}
		return true
	})
	t.Logf("fast endpoint finished %d deliveries in %s while slow endpoint hung", n, time.Since(start).Round(time.Millisecond))

	if peak := slow.peakInFlight(); peak < 1 || peak > 4 {
		t.Errorf("slow endpoint peak in-flight = %d, want 1..4 (per-endpoint cap)", peak)
	}
	for _, id := range slowIDs {
		if s := getDelivery(t, ctx, id).Status; s == "succeeded" || s == "failed" {
			t.Errorf("slow delivery %s is %s while its endpoint hangs", id, s)
		}
	}

	// The service abandons hung attempts after 10s and schedules a retry.
	var timedOut attempt
	waitFor(t, 20*time.Second, "a slow attempt to time out", func() bool {
		for _, id := range slowIDs {
			d := getDelivery(t, ctx, id)
			if len(d.Attempts) > 0 {
				timedOut = d.Attempts[0]
				return d.Status == "pending" || d.Status == "in_flight"
			}
		}
		return false
	})
	if timedOut.StatusCode != 0 || timedOut.Error == "" || timedOut.Outcome != "retry" {
		t.Errorf("timed-out attempt = %+v, want status 0, an error and outcome retry", timedOut)
	}
	if timedOut.DurationMS < 9_000 || timedOut.DurationMS > 12_000 {
		t.Errorf("timed-out attempt took %dms, want ~10s timeout", timedOut.DurationMS)
	}
}

func TestPermanentFailureAndRedeliver(t *testing.T) {
	ctx := t.Context()
	typ := unique("redeliver")
	m := newMerchant(t, respond(http.StatusBadRequest))
	epID := registerEndpoint(t, ctx, m.url, typ)
	dlvID := postEvent(t, ctx, unique("evt_redeliver"), typ)[epID]

	d := waitForStatus(t, ctx, dlvID, "failed", 10*time.Second)
	if len(d.Attempts) != 1 || d.Attempts[0].StatusCode != 400 || d.Attempts[0].Outcome != "fail" || d.NextAttemptAt != nil {
		t.Fatalf("delivery = %+v, want failed after one 400 attempt without retry", d)
	}

	// Merchant fixes their endpoint; operator redelivers.
	m.setHandler(respond(http.StatusOK))
	r := call(t, ctx, http.MethodPost, "/deliveries/"+dlvID+"/redeliver", nil)
	expectCode(t, r, http.StatusAccepted)
	assertFields(t, "redelivered", r.obj, deliveryFields...)
	if r.obj["status"] != "pending" {
		t.Errorf("status after redeliver = %v, want pending", r.obj["status"])
	}

	d = waitForStatus(t, ctx, dlvID, "succeeded", 10*time.Second)
	if len(d.Attempts) != 2 || d.Attempts[0].StatusCode != 400 || d.Attempts[1].StatusCode != 200 || d.Attempts[1].Number != 2 {
		t.Fatalf("attempts = %+v, want history [400, 200]", d.Attempts)
	}
	if n := len(m.received()); n != 2 {
		t.Errorf("merchant received %d requests, want 2", n)
	}

	again := call(t, ctx, http.MethodPost, "/deliveries/"+dlvID+"/redeliver", nil)
	expectCode(t, again, http.StatusConflict)
	notFound := call(t, ctx, http.MethodPost, "/deliveries/"+unique("dlv_nope")+"/redeliver", nil)
	expectCode(t, notFound, http.StatusNotFound)
}

func TestRedirectNotFollowed(t *testing.T) {
	ctx := t.Context()
	typ := unique("redirect")
	target := newMerchant(t, respond(http.StatusOK))
	m := newMerchant(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.url, http.StatusFound)
	})
	epID := registerEndpoint(t, ctx, m.url, typ)
	dlvID := postEvent(t, ctx, unique("evt_redirect"), typ)[epID]

	d := waitForStatus(t, ctx, dlvID, "failed", 10*time.Second)
	if d.Attempts[0].StatusCode != http.StatusFound {
		t.Errorf("attempt status = %d, want 302", d.Attempts[0].StatusCode)
	}
	if n := len(target.received()); n != 0 {
		t.Errorf("redirect target received %d requests, want 0", n)
	}
}

// TestServerErrorIsRetried: a 5xx is recorded and rescheduled, never dropped.
// Whether the retry is observed depends on the service's RETRY_SCHEDULE.
func TestServerErrorIsRetried(t *testing.T) {
	ctx := t.Context()
	typ := unique("retry")
	m := newMerchant(t, respond(http.StatusServiceUnavailable))
	epID := registerEndpoint(t, ctx, m.url, typ)
	dlvID := postEvent(t, ctx, unique("evt_retry"), typ)[epID]

	var d delivery
	waitFor(t, 10*time.Second, "first attempt", func() bool {
		d = getDelivery(t, ctx, dlvID)
		return len(d.Attempts) > 0
	})
	a := d.Attempts[0]
	if a.StatusCode != 503 || a.Outcome != "retry" || d.Status != "pending" || d.NextAttemptAt == nil {
		t.Fatalf("delivery = %+v, want pending retry after 503", d)
	}
	finished := a.StartedAt.Add(time.Duration(a.DurationMS) * time.Millisecond)
	wait := d.NextAttemptAt.Sub(finished)
	if wait <= 0 {
		t.Fatalf("next_attempt_at %v is not after the attempt finished (%v)", d.NextAttemptAt, finished)
	}

	t.Run("retry succeeds", func(t *testing.T) {
		if wait > 15*time.Second {
			t.Skipf("first retry is scheduled %s out (default ~1m schedule); start the service with a short RETRY_SCHEDULE to observe it", wait.Round(time.Second))
		}
		m.setHandler(respond(http.StatusOK))
		d := waitForStatus(t, ctx, dlvID, "succeeded", wait+10*time.Second)
		if len(d.Attempts) != 2 {
			t.Errorf("attempts = %d, want 2", len(d.Attempts))
		}
	})
}

// TestRetryAfterHonored: a 429 with Retry-After: 3600 schedules the retry an
// hour out, longer than any first schedule step, and under the 8h cap.
func TestRetryAfterHonored(t *testing.T) {
	ctx := t.Context()
	typ := unique("retryafter")
	m := newMerchant(t, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "3600")
		w.WriteHeader(http.StatusTooManyRequests)
	})
	epID := registerEndpoint(t, ctx, m.url, typ)
	dlvID := postEvent(t, ctx, unique("evt_retryafter"), typ)[epID]

	var d delivery
	waitFor(t, 10*time.Second, "first attempt", func() bool {
		d = getDelivery(t, ctx, dlvID)
		return len(d.Attempts) > 0
	})
	a := d.Attempts[0]
	if a.StatusCode != 429 || d.Status != "pending" || d.NextAttemptAt == nil {
		t.Fatalf("delivery = %+v, want pending after 429", d)
	}
	finished := a.StartedAt.Add(time.Duration(a.DurationMS) * time.Millisecond)
	if wait := d.NextAttemptAt.Sub(finished); wait < time.Hour-5*time.Second || wait > time.Hour+5*time.Second {
		t.Errorf("retry scheduled %s after the attempt, want ~1h from Retry-After", wait)
	}
}
