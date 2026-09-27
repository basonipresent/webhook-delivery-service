package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"webhook-delivery-service/internal/store"
)

var t0 = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

const secret = "0123456789abcdef"

type apiEnv struct {
	store   *store.Store
	handler http.Handler
}

func newAPI(t *testing.T) *apiEnv {
	t.Helper()
	st := store.New()
	return &apiEnv{store: st, handler: New(st, func() time.Time { return t0 }, nil)}
}

func (e *apiEnv) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	var out map[string]any
	if rec.Body.Len() > 0 && strings.HasPrefix(rec.Header().Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
			t.Fatalf("%s %s: decode response %q: %v", method, path, rec.Body.String(), err)
		}
	}
	return rec.Code, out
}

func (e *apiEnv) endpoint(t *testing.T, types ...string) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"url": "https://merchant.example/hook", "event_types": types, "secret": secret})
	if err != nil {
		t.Fatal(err)
	}
	code, out := e.do(t, http.MethodPost, "/endpoints", string(b))
	if code != http.StatusCreated {
		t.Fatalf("create endpoint: %d %v", code, out)
	}
	return out["id"].(string)
}

func eventJSON(id, typ, payload string) string {
	return fmt.Sprintf(`{"event_id":%q,"type":%q,"created_at":"2026-09-27T11:59:00Z","payload":%s}`, id, typ, payload)
}

func keys(m map[string]any) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	slices.Sort(ks)
	return ks
}

func assertKeys(t *testing.T, what string, m map[string]any, want ...string) {
	t.Helper()
	slices.Sort(want)
	if got := keys(m); !slices.Equal(got, want) {
		t.Errorf("%s fields = %v, want %v", what, got, want)
	}
}

func TestCreateEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{"valid https", `{"url":"https://m.example/hook","event_types":["a","b"],"secret":"0123456789abcdef"}`, 201},
		{"valid http with port", `{"url":"http://localhost:9000/x","event_types":["a"],"secret":"0123456789abcdef"}`, 201},
		{"malformed json", `{"url":`, 400},
		{"trailing data", `{"url":"https://m.example","event_types":["a"],"secret":"0123456789abcdef"} {}`, 400},
		{"wrong field type", `{"url":1,"event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"missing url", `{"event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"relative url", `{"url":"/hook","event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"non-http scheme", `{"url":"ftp://m.example","event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"no host", `{"url":"https://","event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"unparsable url", `{"url":"http://bad host/","event_types":["a"],"secret":"0123456789abcdef"}`, 400},
		{"missing event_types", `{"url":"https://m.example","secret":"0123456789abcdef"}`, 400},
		{"empty event_types", `{"url":"https://m.example","event_types":[],"secret":"0123456789abcdef"}`, 400},
		{"empty event type string", `{"url":"https://m.example","event_types":["a",""],"secret":"0123456789abcdef"}`, 400},
		{"secret too short", `{"url":"https://m.example","event_types":["a"],"secret":"0123456789abcde"}`, 400},
		{"missing secret", `{"url":"https://m.example","event_types":["a"]}`, 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			code, out := newAPI(t).do(t, http.MethodPost, "/endpoints", tt.body)
			if code != tt.wantCode {
				t.Fatalf("code = %d (%v), want %d", code, out, tt.wantCode)
			}
			if code != http.StatusCreated {
				assertKeys(t, "error", out, "error")
				return
			}
			assertKeys(t, "endpoint", out, "id", "url", "event_types", "created_at")
			if out["created_at"] != t0.Format(time.RFC3339) {
				t.Errorf("created_at = %v, want %s", out["created_at"], t0.Format(time.RFC3339))
			}
		})
	}
}

func TestCreateEndpointNeverReturnsSecret(t *testing.T) {
	e := newAPI(t)
	req := httptest.NewRequest(http.MethodPost, "/endpoints",
		strings.NewReader(`{"url":"https://m.example","event_types":["a"],"secret":"super-secret-value-123"}`))
	rec := httptest.NewRecorder()
	e.handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("code = %d", rec.Code)
	}
	if strings.Contains(rec.Body.String(), "super-secret-value-123") {
		t.Fatalf("response leaks secret: %s", rec.Body.String())
	}
}

func TestCreateEvent(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		wantCode int
	}{
		{"valid", eventJSON("evt_1", "order.paid", `{"amount":10}`), 202},
		{"created_at with fraction and offset", `{"event_id":"e","type":"order.paid","created_at":"2026-09-27T11:59:00.123+02:00","payload":{}}`, 202},
		{"malformed json", `{"event_id":`, 400},
		{"missing event_id", `{"type":"order.paid","created_at":"2026-09-27T11:59:00Z","payload":{}}`, 400},
		{"event_id with space", eventJSON("evt 1", "order.paid", `{}`), 400},
		{"missing type", `{"event_id":"e","created_at":"2026-09-27T11:59:00Z","payload":{}}`, 400},
		{"missing created_at", `{"event_id":"e","type":"order.paid","payload":{}}`, 400},
		{"created_at not RFC 3339", `{"event_id":"e","type":"order.paid","created_at":"27/09/2026","payload":{}}`, 400},
		{"missing payload", `{"event_id":"e","type":"order.paid","created_at":"2026-09-27T11:59:00Z"}`, 400},
		{"null payload", eventJSON("e", "order.paid", `null`), 400},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := newAPI(t)
			e.endpoint(t, "order.paid")
			code, out := e.do(t, http.MethodPost, "/events", tt.body)
			if code != tt.wantCode {
				t.Fatalf("code = %d (%v), want %d", code, out, tt.wantCode)
			}
			if code != http.StatusAccepted {
				assertKeys(t, "error", out, "error")
				return
			}
			assertKeys(t, "event", out, "event_id", "deliveries")
			ds := out["deliveries"].([]any)
			if len(ds) != 1 {
				t.Fatalf("deliveries = %v, want 1", ds)
			}
			d := ds[0].(map[string]any)
			assertKeys(t, "delivery summary", d, "id", "endpoint_id", "status")
			if d["status"] != "pending" {
				t.Errorf("status = %v, want pending", d["status"])
			}
		})
	}
}

func TestCreateEventBodyTooLarge(t *testing.T) {
	e := newAPI(t)
	big := eventJSON("e", "a", `"`+strings.Repeat("x", maxBodyBytes)+`"`)
	code, out := e.do(t, http.MethodPost, "/events", big)
	if code != http.StatusRequestEntityTooLarge {
		t.Fatalf("code = %d (%v), want 413", code, out)
	}
	assertKeys(t, "error", out, "error")
}

func TestCreateEventNoSubscribers(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "other")
	code, out := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{}`))
	if code != http.StatusAccepted {
		t.Fatalf("code = %d, want 202", code)
	}
	if ds := out["deliveries"].([]any); len(ds) != 0 {
		t.Fatalf("deliveries = %v, want empty array", ds)
	}
}

func TestCreateEventReplayAndConflict(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "order.paid")
	code, first := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{"a":1}`))
	if code != http.StatusAccepted {
		t.Fatalf("first: %d", code)
	}

	code, again := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{ "a": 1 }`))
	if code != http.StatusOK {
		t.Fatalf("identical replay: code %d, want 200", code)
	}
	if fmt.Sprint(again) != fmt.Sprint(first) {
		t.Errorf("replay body %v, want %v", again, first)
	}

	code, out := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{"a":2}`))
	if code != http.StatusConflict {
		t.Fatalf("conflict: code %d (%v), want 409", code, out)
	}
	assertKeys(t, "error", out, "error")
}

func TestCreateEventConcurrentReplays(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "order.paid")
	const n = 30
	codes := make(chan int, n)
	var wg sync.WaitGroup
	for range n {
		wg.Go(func() {
			req := httptest.NewRequest(http.MethodPost, "/events", strings.NewReader(eventJSON("evt_1", "order.paid", `{}`)))
			rec := httptest.NewRecorder()
			e.handler.ServeHTTP(rec, req)
			codes <- rec.Code
		})
	}
	wg.Wait()
	close(codes)
	counts := map[int]int{}
	for c := range codes {
		counts[c]++
	}
	if counts[http.StatusAccepted] != 1 || counts[http.StatusOK] != n-1 {
		t.Fatalf("status counts = %v, want one 202 and %d 200s", counts, n-1)
	}
}

func TestGetEvent(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "order.paid")
	if code, out := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{"amount":10}`)); code != 202 {
		t.Fatalf("create: %d %v", code, out)
	}
	// Record one failed attempt so the history is visible.
	jobs := e.store.ClaimDue(t0, 1)
	a := store.Attempt{Number: 1, StartedAt: t0, Duration: 1500 * time.Millisecond, StatusCode: 503, Outcome: "retry"}
	if err := e.store.Complete(jobs[0].DeliveryID, a, store.StatusPending, t0.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	code, out := e.do(t, http.MethodGet, "/events/evt_1", "")
	if code != http.StatusOK {
		t.Fatalf("code = %d (%v)", code, out)
	}
	assertKeys(t, "event", out, "event_id", "type", "created_at", "received_at", "payload", "deliveries")
	if p := out["payload"].(map[string]any); p["amount"] != float64(10) {
		t.Errorf("payload = %v", p)
	}
	d := out["deliveries"].([]any)[0].(map[string]any)
	assertKeys(t, "delivery", d, "id", "event_id", "endpoint_id", "status", "next_attempt_at", "attempts")
	if d["status"] != "pending" || d["next_attempt_at"] != t0.Add(time.Minute).Format(time.RFC3339) {
		t.Errorf("delivery = %v", d)
	}
	at := d["attempts"].([]any)[0].(map[string]any)
	assertKeys(t, "attempt", at, "number", "started_at", "duration_ms", "status_code", "error", "outcome")
	if at["duration_ms"] != float64(1500) || at["status_code"] != float64(503) || at["outcome"] != "retry" {
		t.Errorf("attempt = %v", at)
	}
}

func TestGetDelivery(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "order.paid")
	_, created := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{}`))
	id := created["deliveries"].([]any)[0].(map[string]any)["id"].(string)

	code, out := e.do(t, http.MethodGet, "/deliveries/"+id, "")
	if code != http.StatusOK {
		t.Fatalf("code = %d", code)
	}
	assertKeys(t, "delivery", out, "id", "event_id", "endpoint_id", "status", "next_attempt_at", "attempts")
	if atts := out["attempts"].([]any); len(atts) != 0 {
		t.Errorf("attempts = %v, want empty array", atts)
	}

	jobs := e.store.ClaimDue(t0, 1)
	if err := e.store.Complete(jobs[0].DeliveryID, store.Attempt{Number: 1, StatusCode: 200, Outcome: "success"}, store.StatusSucceeded, time.Time{}); err != nil {
		t.Fatal(err)
	}
	_, out = e.do(t, http.MethodGet, "/deliveries/"+id, "")
	if out["status"] != "succeeded" || out["next_attempt_at"] != nil {
		t.Errorf("after success: status %v next_attempt_at %v, want succeeded and null", out["status"], out["next_attempt_at"])
	}
}

func TestNotFoundAndMethods(t *testing.T) {
	tests := []struct {
		method, path string
		wantCode     int
	}{
		{http.MethodGet, "/events/nope", 404},
		{http.MethodGet, "/deliveries/nope", 404},
		{http.MethodGet, "/healthz", 200},
		{http.MethodGet, "/endpoints", 405},
		{http.MethodDelete, "/events/x", 405},
		{http.MethodGet, "/unknown", 404},
	}
	for _, tt := range tests {
		t.Run(tt.method+" "+tt.path, func(t *testing.T) {
			code, _ := newAPI(t).do(t, tt.method, tt.path, "")
			if code != tt.wantCode {
				t.Fatalf("code = %d, want %d", code, tt.wantCode)
			}
		})
	}
}

func TestRedeliver(t *testing.T) {
	e := newAPI(t)
	e.endpoint(t, "order.paid")
	_, created := e.do(t, http.MethodPost, "/events", eventJSON("evt_1", "order.paid", `{}`))
	id := created["deliveries"].([]any)[0].(map[string]any)["id"].(string)

	if code, out := e.do(t, http.MethodPost, "/deliveries/"+id+"/redeliver", ""); code != http.StatusConflict {
		t.Fatalf("redeliver pending: %d (%v), want 409", code, out)
	}
	if code, _ := e.do(t, http.MethodPost, "/deliveries/nope/redeliver", ""); code != http.StatusNotFound {
		t.Fatalf("redeliver unknown: %d, want 404", code)
	}

	jobs := e.store.ClaimDue(t0, 1)
	if err := e.store.Complete(jobs[0].DeliveryID, store.Attempt{Number: 1, StatusCode: 400, Outcome: "fail"}, store.StatusFailed, time.Time{}); err != nil {
		t.Fatal(err)
	}

	code, out := e.do(t, http.MethodPost, "/deliveries/"+id+"/redeliver", "")
	if code != http.StatusAccepted {
		t.Fatalf("redeliver failed: %d (%v), want 202", code, out)
	}
	assertKeys(t, "delivery", out, "id", "event_id", "endpoint_id", "status", "next_attempt_at", "attempts")
	if out["status"] != "pending" || out["next_attempt_at"] != t0.Format(time.RFC3339) || len(out["attempts"].([]any)) != 1 {
		t.Errorf("after redeliver: %v, want pending now with history kept", out)
	}

	if code, _ := e.do(t, http.MethodPost, "/deliveries/"+id+"/redeliver", ""); code != http.StatusConflict {
		t.Fatalf("second redeliver: %d, want 409", code)
	}
	if code, _ := e.do(t, http.MethodGet, "/deliveries/"+id+"/redeliver", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET redeliver: %d, want 405", code)
	}
}
