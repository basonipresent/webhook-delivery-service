package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"webhook-delivery-service/internal/dispatch"
)

func TestLoadConfig(t *testing.T) {
	tests := []struct {
		name         string
		env          map[string]string
		wantAddr     string
		wantSchedule []time.Duration // nil = default schedule
		wantErr      bool
	}{
		{"defaults", nil, ":8080", nil, false},
		{"port", map[string]string{"PORT": "9090"}, ":9090", nil, false},
		{"port max", map[string]string{"PORT": "65535"}, ":65535", nil, false},
		{"port not a number", map[string]string{"PORT": "http"}, "", nil, true},
		{"port zero", map[string]string{"PORT": "0"}, "", nil, true},
		{"port too large", map[string]string{"PORT": "65536"}, "", nil, true},
		{"port negative", map[string]string{"PORT": "-1"}, "", nil, true},
		{"retry schedule", map[string]string{"RETRY_SCHEDULE": "1s,2s,4s"}, ":8080", []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, false},
		{"retry schedule invalid", map[string]string{"RETRY_SCHEDULE": "1s,soon"}, "", nil, true},
		{"retry schedule non-positive", map[string]string{"RETRY_SCHEDULE": "0s"}, "", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := loadConfig(func(k string) string { return tt.env[k] })
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if cfg.addr != tt.wantAddr {
				t.Errorf("addr = %q, want %q", cfg.addr, tt.wantAddr)
			}
			want := tt.wantSchedule
			if want == nil {
				want = dispatch.DefaultRetryPolicy().Schedule
			}
			if !slices.Equal(cfg.dispatch.Policy.Schedule, want) {
				t.Errorf("schedule = %v, want %v", cfg.dispatch.Policy.Schedule, want)
			}
			if cfg.dispatch.Policy.Jitter == nil || cfg.dispatch.AttemptTimeout != 10*time.Second {
				t.Errorf("RETRY_SCHEDULE must keep default jitter and 10s timeout: %+v", cfg.dispatch)
			}
		})
	}
}

// e2eMerchant records deliveries and verifies each signature. When
// failFirst is set, the first request for every event gets a 500.
type e2eMerchant struct {
	*httptest.Server
	secret    string
	failFirst bool

	mu      sync.Mutex
	seen    map[string]int // event_id -> requests
	badSigs int
}

func newE2EMerchant(t *testing.T, secret string, failFirst bool) *e2eMerchant {
	t.Helper()
	m := &e2eMerchant{secret: secret, failFirst: failFirst, seen: map[string]int{}}
	m.Server = httptest.NewServer(http.HandlerFunc(m.handle))
	t.Cleanup(m.Close)
	return m
}

func (m *e2eMerchant) handle(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	ts, tsErr := strconv.ParseInt(r.Header.Get(dispatch.HeaderTimestamp), 10, 64)
	valid := err == nil && tsErr == nil &&
		r.Header.Get(dispatch.HeaderSignature) == "v1="+dispatch.Sign([]byte(m.secret), ts, body)

	var env struct {
		EventID string `json:"event_id"`
	}
	if jsonErr := json.Unmarshal(body, &env); jsonErr != nil || env.EventID != r.Header.Get(dispatch.HeaderEventID) {
		valid = false
	}

	m.mu.Lock()
	m.seen[env.EventID]++
	n := m.seen[env.EventID]
	if !valid {
		m.badSigs++
	}
	m.mu.Unlock()

	if m.failFirst && n == 1 {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (m *e2eMerchant) stats() (map[string]int, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := make(map[string]int, len(m.seen))
	for k, v := range m.seen {
		seen[k] = v
	}
	return seen, m.badSigs
}

func postJSON(ctx context.Context, url string, v any) (int, []byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return 0, nil, fmt.Errorf("marshal: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return 0, nil, fmt.Errorf("new request: %w", err)
	}
	return doRequest(req)
}

func getJSON(ctx context.Context, url string) (int, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, nil, fmt.Errorf("new request: %w", err)
	}
	return doRequest(req)
}

func doRequest(req *http.Request) (int, []byte, error) {
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("%s %s: %w", req.Method, req.URL, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, nil, fmt.Errorf("read body: %w", err)
	}
	return resp.StatusCode, body, nil
}

// TestEndToEnd runs the real server and dispatcher, registers three merchants
// (one fails every first attempt), posts events concurrently with duplicate
// replays, and checks every delivery succeeds with a valid signature.
func TestEndToEnd(t *testing.T) {
	const (
		producers      = 10
		eventsPerProd  = 5
		totalEvents    = producers * eventsPerProd
		merchantSecret = "0123456789abcdef"
	)
	merchants := []*e2eMerchant{
		newE2EMerchant(t, merchantSecret, false),
		newE2EMerchant(t, merchantSecret, true),
		newE2EMerchant(t, merchantSecret, false),
	}

	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.dispatch.Policy = dispatch.RetryPolicy{Schedule: []time.Duration{20 * time.Millisecond, 50 * time.Millisecond}}
	cfg.dispatch.PollInterval = 5 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	base := "http://" + ln.Addr().String()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	serveErr := make(chan error, 1)
	go func() { serveErr <- serve(ctx, ln, cfg, slog.New(slog.DiscardHandler)) }()

	for _, m := range merchants {
		code, body, err := postJSON(ctx, base+"/endpoints", map[string]any{
			"url": m.URL, "event_types": []string{"order.paid"}, "secret": merchantSecret,
		})
		if err != nil || code != http.StatusCreated {
			t.Fatalf("register endpoint: %d %s %v", code, body, err)
		}
	}

	// Each producer sends its events twice; the replay must not add deliveries.
	var wg sync.WaitGroup
	errs := make(chan error, producers*eventsPerProd*2)
	for p := range producers {
		wg.Go(func() {
			for i := range eventsPerProd {
				ev := map[string]any{
					"event_id":   fmt.Sprintf("evt_%d_%d", p, i),
					"type":       "order.paid",
					"created_at": "2026-09-27T12:00:00Z",
					"payload":    map[string]any{"producer": p, "seq": i},
				}
				for attempt, want := range []int{http.StatusAccepted, http.StatusOK} {
					code, body, err := postJSON(ctx, base+"/events", ev)
					if err != nil || code != want {
						errs <- fmt.Errorf("post %v (send %d): %d %s %v", ev["event_id"], attempt+1, code, body, err)
					}
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if t.Failed() {
		t.FailNow()
	}

	// Wait until every event's deliveries have succeeded.
	deadline := time.Now().Add(10 * time.Second)
	var pending []string
	for {
		pending = pending[:0]
		for p := range producers {
			for i := range eventsPerProd {
				id := fmt.Sprintf("evt_%d_%d", p, i)
				code, body, err := getJSON(ctx, base+"/events/"+id)
				if err != nil || code != http.StatusOK {
					t.Fatalf("get %s: %d %s %v", id, code, body, err)
				}
				var ev struct {
					Deliveries []struct {
						Status   string            `json:"status"`
						Attempts []json.RawMessage `json:"attempts"`
					} `json:"deliveries"`
				}
				if err := json.Unmarshal(body, &ev); err != nil {
					t.Fatal(err)
				}
				if len(ev.Deliveries) != len(merchants) {
					t.Fatalf("%s: %d deliveries, want %d (replays must not add any)", id, len(ev.Deliveries), len(merchants))
				}
				for _, d := range ev.Deliveries {
					if d.Status != "succeeded" {
						pending = append(pending, id+":"+d.Status)
					}
				}
			}
		}
		if len(pending) == 0 || time.Now().After(deadline) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if len(pending) > 0 {
		t.Fatalf("%d deliveries not succeeded before deadline, e.g. %v", len(pending), pending[:min(5, len(pending))])
	}

	for i, m := range merchants {
		seen, badSigs := m.stats()
		if badSigs != 0 {
			t.Errorf("merchant %d: %d requests with invalid signature or event id", i, badSigs)
		}
		if len(seen) != totalEvents {
			t.Errorf("merchant %d: received %d distinct events, want %d", i, len(seen), totalEvents)
		}
		minHits := 1
		if m.failFirst {
			minHits = 2
		}
		for id, n := range seen {
			if n < minHits {
				t.Errorf("merchant %d: event %s received %d times, want >= %d", i, id, n, minHits)
			}
		}
	}

	cancel()
	select {
	case err := <-serveErr:
		if err != nil {
			t.Fatalf("serve returned %v, want nil on shutdown", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("serve did not return after cancel")
	}
	if _, _, err := getJSON(context.Background(), base+"/healthz"); err == nil {
		t.Error("server still accepting requests after shutdown")
	}
}

func TestServeRejectsInvalidDispatchConfig(t *testing.T) {
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	cfg.dispatch.PerEndpointLimit = 0
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	if err := serve(t.Context(), ln, cfg, slog.New(slog.DiscardHandler)); err == nil {
		t.Fatal("serve accepted an invalid dispatcher config")
	}
}
