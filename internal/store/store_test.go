package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var t0 = time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)

func mustEndpoint(t *testing.T, s *Store, types ...string) Endpoint {
	t.Helper()
	ep, err := s.CreateEndpoint(Endpoint{URL: "http://example.test/hook", Secret: "0123456789abcdef", EventTypes: types, CreatedAt: t0})
	if err != nil {
		t.Fatalf("CreateEndpoint: %v", err)
	}
	return ep
}

func event(id, typ, payload string) Event {
	return Event{ID: id, Type: typ, CreatedAt: t0, Payload: json.RawMessage(payload)}
}

func TestCreateEndpoint(t *testing.T) {
	valid := Endpoint{URL: "http://example.test", Secret: "0123456789abcdef", EventTypes: []string{"a"}}
	tests := []struct {
		name    string
		mutate  func(*Endpoint)
		wantErr error
	}{
		{"valid", func(*Endpoint) {}, nil},
		{"missing url", func(e *Endpoint) { e.URL = "" }, ErrInvalid},
		{"missing secret", func(e *Endpoint) { e.Secret = "" }, ErrInvalid},
		{"nil event types", func(e *Endpoint) { e.EventTypes = nil }, ErrInvalid},
		{"empty event types", func(e *Endpoint) { e.EventTypes = []string{} }, ErrInvalid},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			in := copyEndpoint(valid)
			tt.mutate(&in)
			got, err := New().CreateEndpoint(in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			if tt.wantErr == nil && got.ID == "" {
				t.Fatal("expected generated ID")
			}
		})
	}
}

func TestCreateEndpointUniqueIDsAndCopiesInput(t *testing.T) {
	s := New()
	types := []string{"a"}
	ep1, err := s.CreateEndpoint(Endpoint{URL: "u", Secret: "s", EventTypes: types})
	if err != nil {
		t.Fatal(err)
	}
	types[0] = "b" // caller mutation must not change the stored subscription

	ep2 := mustEndpoint(t, s, "a")
	if ep1.ID == ep2.ID {
		t.Fatalf("duplicate IDs %q", ep1.ID)
	}
	_, ds, _, err := s.CreateEvent(event("e1", "a", `{}`), t0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ds) != 2 {
		t.Fatalf("got %d deliveries, want 2 (stored types must be copied)", len(ds))
	}
}

func TestCreateEventFanOut(t *testing.T) {
	s := New()
	a := mustEndpoint(t, s, "order.paid", "order.refunded")
	b := mustEndpoint(t, s, "order.paid", "order.paid") // duplicate type: still one delivery
	mustEndpoint(t, s, "user.created")

	ev, ds, created, err := s.CreateEvent(event("evt_1", "order.paid", `{"amount": 10}`), t0)
	if err != nil {
		t.Fatalf("CreateEvent: %v", err)
	}
	if !created {
		t.Fatal("created = false, want true")
	}
	if !ev.ReceivedAt.Equal(t0) {
		t.Errorf("ReceivedAt = %v, want %v", ev.ReceivedAt, t0)
	}
	got := map[string]bool{}
	for _, d := range ds {
		got[d.EndpointID] = true
		if d.EventID != "evt_1" || d.Status != StatusPending || !d.NextAttemptAt.Equal(t0) || len(d.Attempts) != 0 {
			t.Errorf("unexpected delivery %+v", d)
		}
	}
	if len(ds) != 2 || !got[a.ID] || !got[b.ID] {
		t.Fatalf("deliveries to %v, want exactly %s and %s", got, a.ID, b.ID)
	}
	if len(s.pending) != 2 {
		t.Errorf("pending = %d, want 2", len(s.pending))
	}
}

func TestCreateEventBody(t *testing.T) {
	s := New()
	ev, _, _, err := s.CreateEvent(event("evt_1", "order.paid", `{"amount":10}`), t0)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"event_id":"evt_1","type":"order.paid","created_at":"2026-09-27T12:00:00Z","payload":{"amount":10}}`
	if string(ev.Body) != want {
		t.Errorf("Body = %s\nwant   %s", ev.Body, want)
	}
}

func TestCreateEventNoSubscribers(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "other")
	_, ds, created, err := s.CreateEvent(event("evt_1", "order.paid", `{}`), t0)
	if err != nil || !created {
		t.Fatalf("created=%v err=%v, want created without error", created, err)
	}
	if len(ds) != 0 {
		t.Fatalf("got %d deliveries, want 0", len(ds))
	}
	if _, _, err := s.GetEvent("evt_1"); err != nil {
		t.Fatalf("GetEvent: %v", err)
	}
}

func TestCreateEventInvalid(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
	}{
		{"missing id", event("", "a", `{}`)},
		{"missing type", event("e", "", `{}`)},
		{"missing created_at", Event{ID: "e", Type: "a", Payload: json.RawMessage(`{}`)}},
		{"missing payload", event("e", "a", ``)},
		{"whitespace payload", event("e", "a", "  \n")},
		{"null payload", event("e", "a", `null`)},
		{"null payload with whitespace", event("e", "a", " null\n")},
		{"invalid payload json", event("e", "a", `{"x":`)},
		{"id too long", event(strings.Repeat("x", MaxEventIDLen+1), "a", `{}`)},
		{"id with newline", event("e\n1", "a", `{}`)},
		{"id with carriage return", event("e\r1", "a", `{}`)},
		{"id with space", event("e 1", "a", `{}`)},
		{"id with tab", event("e\t1", "a", `{}`)},
		{"id with DEL", event("e\x7f", "a", `{}`)},
		{"id with non-ASCII", event("évt", "a", `{}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			_, _, _, err := s.CreateEvent(tt.ev, t0)
			if !errors.Is(err, ErrInvalid) {
				t.Fatalf("err = %v, want ErrInvalid", err)
			}
			if len(s.events) != 0 || len(s.deliveries) != 0 {
				t.Fatal("invalid event must not be stored")
			}
		})
	}
}

func TestCreateEventValidEdgeInputs(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
	}{
		{"id at max length", event(strings.Repeat("x", MaxEventIDLen), "a", `{}`)},
		{"id with visible punctuation", event("evt_01:a/b-c.d~!", "a", `{}`)},
		{"scalar payload", event("e", "a", `0`)},
		{"string payload null", event("e", "a", `"null"`)},
		{"empty object payload", event("e", "a", `{}`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, created, err := New().CreateEvent(tt.ev, t0); err != nil || !created {
				t.Fatalf("created=%v err=%v, want created", created, err)
			}
		})
	}
}

func TestCreateEventReplay(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	_, first, _, err := s.CreateEvent(event("e1", "a", `{"x":1,"y":[1,2]}`), t0)
	if err != nil {
		t.Fatal(err)
	}

	later := t0.Add(time.Minute)
	ev, again, created, err := s.CreateEvent(event("e1", "a", ` { "x": 1, "y": [1, 2] } `), later)
	if err != nil {
		t.Fatalf("identical replay: %v", err)
	}
	if created {
		t.Fatal("identical replay: created = true, want false")
	}
	if !ev.ReceivedAt.Equal(t0) {
		t.Errorf("replay changed ReceivedAt to %v", ev.ReceivedAt)
	}
	if len(again) != 1 || again[0].ID != first[0].ID {
		t.Fatalf("replay deliveries = %+v, want existing %+v", again, first)
	}
	if len(s.deliveries) != 1 {
		t.Fatalf("replay created deliveries: %d total", len(s.deliveries))
	}
}

func TestCreateEventConflict(t *testing.T) {
	tests := []struct {
		name string
		ev   Event
	}{
		{"different type", event("e1", "b", `{"x":1}`)},
		{"different payload", event("e1", "a", `{"x":2}`)},
		{"different created_at", Event{ID: "e1", Type: "a", CreatedAt: t0.Add(time.Second), Payload: json.RawMessage(`{"x":1}`)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			if _, _, _, err := s.CreateEvent(event("e1", "a", `{"x":1}`), t0); err != nil {
				t.Fatal(err)
			}
			_, _, _, err := s.CreateEvent(tt.ev, t0)
			if !errors.Is(err, ErrConflict) {
				t.Fatalf("err = %v, want ErrConflict", err)
			}
		})
	}
}

func TestCreateEventSameInstantDifferentZoneIsReplay(t *testing.T) {
	s := New()
	if _, _, _, err := s.CreateEvent(event("e1", "a", `{}`), t0); err != nil {
		t.Fatal(err)
	}
	ev := event("e1", "a", `{}`)
	ev.CreatedAt = t0.In(time.FixedZone("X", 3600))
	if _, _, created, err := s.CreateEvent(ev, t0); err != nil || created {
		t.Fatalf("created=%v err=%v, want replay", created, err)
	}
}

func TestGettersNotFound(t *testing.T) {
	s := New()
	if _, _, err := s.GetEvent("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetEvent err = %v, want ErrNotFound", err)
	}
	if _, err := s.GetDelivery("nope"); !errors.Is(err, ErrNotFound) {
		t.Errorf("GetDelivery err = %v, want ErrNotFound", err)
	}
}

func TestGettersReturnDeepCopies(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	in := event("e1", "a", `{"x":1}`)
	_, ds, _, err := s.CreateEvent(in, t0)
	if err != nil {
		t.Fatal(err)
	}
	in.Payload[2] = 'Z' // mutating the input must not reach the store

	ev, _, err := s.GetEvent("e1")
	if err != nil {
		t.Fatal(err)
	}
	ev.Body[0] = 'X'
	ev.Payload[0] = 'X'

	s.mu.Lock()
	s.deliveries[ds[0].ID].Attempts = []Attempt{{Number: 1}}
	s.mu.Unlock()
	d, err := s.GetDelivery(ds[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	d.Attempts[0].Number = 99
	d.Status = StatusFailed

	ev2, ds2, err := s.GetEvent("e1")
	if err != nil {
		t.Fatal(err)
	}
	if string(ev2.Payload) != `{"x":1}` || ev2.Body[0] != '{' {
		t.Errorf("event mutated through copy: payload=%s body=%s", ev2.Payload, ev2.Body)
	}
	if ds2[0].Attempts[0].Number != 1 || ds2[0].Status != StatusPending {
		t.Errorf("delivery mutated through copy: %+v", ds2[0])
	}
}

func TestCreateEventConcurrentSameID(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	const n = 50
	var wg sync.WaitGroup
	results := make(chan bool, n)
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			_, _, created, err := s.CreateEvent(event("e1", "a", `{}`), t0)
			if err != nil {
				errs <- err
				return
			}
			results <- created
		})
	}
	wg.Wait()
	close(results)
	close(errs)
	for err := range errs {
		t.Errorf("CreateEvent: %v", err)
	}
	createdCount := 0
	for c := range results {
		if c {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Errorf("created %d times, want exactly 1", createdCount)
	}
	if len(s.deliveries) != 1 {
		t.Errorf("deliveries = %d, want 1", len(s.deliveries))
	}
}

func TestConcurrentEndpointsAndEvents(t *testing.T) {
	s := New()
	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := range 50 {
		wg.Go(func() {
			if _, err := s.CreateEndpoint(Endpoint{URL: "u", Secret: "s", EventTypes: []string{"a"}}); err != nil {
				errs <- err
			}
		})
		wg.Go(func() {
			id := fmt.Sprintf("e%d", i)
			if _, _, _, err := s.CreateEvent(event(id, "a", `{}`), t0); err != nil {
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("unexpected error: %v", err)
	}
	if len(s.endpoints) != 50 || len(s.events) != 50 {
		t.Fatalf("endpoints=%d events=%d, want 50 each", len(s.endpoints), len(s.events))
	}
}
