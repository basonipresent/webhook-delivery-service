package store

import (
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// newEvent creates event id of type "a" received at now and returns its
// single delivery ID. Fails if the event does not fan out to exactly one endpoint.
func newEvent(t *testing.T, s *Store, id string, now time.Time) string {
	t.Helper()
	_, ds, _, err := s.CreateEvent(event(id, "a", `{}`), now)
	if err != nil {
		t.Fatalf("CreateEvent %s: %v", id, err)
	}
	if len(ds) != 1 {
		t.Fatalf("event %s: %d deliveries, want 1", id, len(ds))
	}
	return ds[0].ID
}

func claimIDs(jobs []Job) []string {
	ids := make([]string, len(jobs))
	for i, j := range jobs {
		ids[i] = j.DeliveryID
	}
	return ids
}

func mustStatus(t *testing.T, s *Store, id string, want Status) Delivery {
	t.Helper()
	d, err := s.GetDelivery(id)
	if err != nil {
		t.Fatalf("GetDelivery: %v", err)
	}
	if d.Status != want {
		t.Fatalf("status = %q, want %q", d.Status, want)
	}
	return d
}

func TestClaimDueRespectsNextAttemptAt(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	id := newEvent(t, s, "e1", t0)

	if jobs := s.ClaimDue(t0.Add(-time.Nanosecond), 10); len(jobs) != 0 {
		t.Fatalf("claimed %v before due", claimIDs(jobs))
	}
	jobs := s.ClaimDue(t0, 10) // due exactly at now is inclusive
	if len(jobs) != 1 || jobs[0].DeliveryID != id {
		t.Fatalf("claimed %v, want [%s]", claimIDs(jobs), id)
	}
	mustStatus(t, s, id, StatusInFlight)
}

func TestClaimDueJobContents(t *testing.T) {
	s := New()
	ep := mustEndpoint(t, s, "a")
	id := newEvent(t, s, "e1", t0)
	ev, _, err := s.GetEvent("e1")
	if err != nil {
		t.Fatal(err)
	}

	jobs := s.ClaimDue(t0, 1)
	if len(jobs) != 1 {
		t.Fatalf("claimed %d, want 1", len(jobs))
	}
	j := jobs[0]
	if j.DeliveryID != id || j.EventID != "e1" || j.EndpointID != ep.ID || j.AttemptNumber != 1 ||
		j.URL != ep.URL || j.Secret != ep.Secret || string(j.Body) != string(ev.Body) {
		t.Fatalf("unexpected job %+v", j)
	}

	j.Body[0] = 'X' // job body is a copy
	ev2, _, err := s.GetEvent("e1")
	if err != nil {
		t.Fatal(err)
	}
	if ev2.Body[0] != '{' {
		t.Fatal("mutating job body changed the stored event")
	}
}

func TestClaimDueOldestFirst(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	late := newEvent(t, s, "late", t0.Add(2*time.Second))
	early := newEvent(t, s, "early", t0)
	mid := newEvent(t, s, "mid", t0.Add(time.Second))

	got := claimIDs(s.ClaimDue(t0.Add(time.Minute), 10))
	want := []string{early, mid, late}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("claim order %v, want %v", got, want)
	}
}

func TestClaimDueOldestFirstUnderCap(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	newEvent(t, s, "late", t0.Add(time.Second))
	early := newEvent(t, s, "early", t0)

	got := claimIDs(s.ClaimDue(t0.Add(time.Minute), 1))
	if len(got) != 1 || got[0] != early {
		t.Fatalf("claimed %v, want [%s]", got, early)
	}
}

func TestClaimDuePerEndpointCap(t *testing.T) {
	s := New()
	slow := mustEndpoint(t, s, "a")
	for i := range 5 {
		if _, _, _, err := s.CreateEvent(event(fmt.Sprintf("e%d", i), "a", `{}`), t0); err != nil {
			t.Fatal(err)
		}
	}
	fast := mustEndpoint(t, s, "a") // registered later: only sees events from now on
	for i := range 3 {
		if _, _, _, err := s.CreateEvent(event(fmt.Sprintf("f%d", i), "a", `{}`), t0); err != nil {
			t.Fatal(err)
		}
	}
	// slow now has 8 pending deliveries, fast has 3.

	perEndpoint := func(jobs []Job) map[string]int {
		n := map[string]int{}
		for _, j := range jobs {
			n[j.EndpointID]++
		}
		return n
	}

	first := s.ClaimDue(t0, 2)
	if n := perEndpoint(first); n[slow.ID] != 2 || n[fast.ID] != 2 {
		t.Fatalf("first claim per endpoint = %v, want 2 each", n)
	}
	if again := s.ClaimDue(t0, 2); len(again) != 0 {
		t.Fatalf("claimed %v while both endpoints at cap", claimIDs(again))
	}

	// Freeing one slow slot allows exactly one more slow claim.
	var slowJob Job
	for _, j := range first {
		if j.EndpointID == slow.ID {
			slowJob = j
			break
		}
	}
	if err := s.Complete(slowJob.DeliveryID, Attempt{Number: 1, Outcome: "success"}, StatusSucceeded, time.Time{}); err != nil {
		t.Fatal(err)
	}
	next := s.ClaimDue(t0, 2)
	if n := perEndpoint(next); len(next) != 1 || n[slow.ID] != 1 {
		t.Fatalf("after freeing a slot claimed %v, want 1 for slow endpoint", n)
	}
}

func TestClaimDueLimitBelowOneTreatedAsOne(t *testing.T) {
	for _, limit := range []int{0, -1} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			s := New()
			mustEndpoint(t, s, "a")
			newEvent(t, s, "e1", t0)
			newEvent(t, s, "e2", t0)
			if jobs := s.ClaimDue(t0, limit); len(jobs) != 1 {
				t.Fatalf("claimed %d, want 1", len(jobs))
			}
		})
	}
}

func TestClaimDueNeverDoubleClaims(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	newEvent(t, s, "e1", t0)
	if jobs := s.ClaimDue(t0, 10); len(jobs) != 1 {
		t.Fatalf("claimed %d, want 1", len(jobs))
	}
	if jobs := s.ClaimDue(t0.Add(time.Hour), 10); len(jobs) != 0 {
		t.Fatalf("in_flight delivery claimed again: %v", claimIDs(jobs))
	}
}

func TestCompleteTransitions(t *testing.T) {
	retryAt := t0.Add(time.Minute)
	tests := []struct {
		name        string
		next        Status
		nextAt      time.Time
		wantNextAt  time.Time
		wantPending bool
	}{
		{"success", StatusSucceeded, retryAt, time.Time{}, false},
		{"retry", StatusPending, retryAt, retryAt, true},
		{"fail", StatusFailed, time.Time{}, time.Time{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			ep := mustEndpoint(t, s, "a")
			id := newEvent(t, s, "e1", t0)
			s.ClaimDue(t0, 1)

			a := Attempt{Number: 1, StartedAt: t0, Duration: time.Second, StatusCode: 500, Error: "boom", Outcome: "x"}
			if err := s.Complete(id, a, tt.next, tt.nextAt); err != nil {
				t.Fatalf("Complete: %v", err)
			}
			d := mustStatus(t, s, id, tt.next)
			if len(d.Attempts) != 1 || d.Attempts[0] != a {
				t.Errorf("attempts = %+v, want [%+v]", d.Attempts, a)
			}
			if !d.NextAttemptAt.Equal(tt.wantNextAt) {
				t.Errorf("NextAttemptAt = %v, want %v", d.NextAttemptAt, tt.wantNextAt)
			}
			if _, ok := s.pending[id]; ok != tt.wantPending {
				t.Errorf("in pending set = %v, want %v", ok, tt.wantPending)
			}
			if n, ok := s.inFlight[ep.ID]; ok {
				t.Errorf("endpoint slot not freed: inFlight = %d", n)
			}
		})
	}
}

func TestCompleteRetryIsClaimedAgainWhenDue(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	id := newEvent(t, s, "e1", t0)
	s.ClaimDue(t0, 1)
	retryAt := t0.Add(time.Minute)
	if err := s.Complete(id, Attempt{Number: 1}, StatusPending, retryAt); err != nil {
		t.Fatal(err)
	}

	if jobs := s.ClaimDue(retryAt.Add(-time.Nanosecond), 1); len(jobs) != 0 {
		t.Fatalf("retry claimed before due: %v", claimIDs(jobs))
	}
	jobs := s.ClaimDue(retryAt, 1)
	if len(jobs) != 1 || jobs[0].AttemptNumber != 2 {
		t.Fatalf("jobs = %+v, want one job with attempt 2", jobs)
	}
}

func TestCompleteErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, s *Store, id string) // brings the delivery into the state under test
		id      func(id string) string
		attempt Attempt
		next    Status
		nextAt  time.Time
		wantErr error
	}{
		{
			name:    "unknown delivery",
			setup:   func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) },
			id:      func(string) string { return "dlv_missing" },
			attempt: Attempt{Number: 1}, next: StatusSucceeded, wantErr: ErrNotFound,
		},
		{
			name:    "pending, never claimed",
			setup:   func(*testing.T, *Store, string) {},
			attempt: Attempt{Number: 1}, next: StatusSucceeded, wantErr: ErrConflict,
		},
		{
			name: "already completed",
			setup: func(t *testing.T, s *Store, id string) {
				s.ClaimDue(t0, 1)
				if err := s.Complete(id, Attempt{Number: 1}, StatusSucceeded, time.Time{}); err != nil {
					t.Fatal(err)
				}
			},
			attempt: Attempt{Number: 2}, next: StatusSucceeded, wantErr: ErrConflict,
		},
		{
			name:    "next in_flight",
			setup:   func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) },
			attempt: Attempt{Number: 1}, next: StatusInFlight, wantErr: ErrInvalid,
		},
		{
			name:    "next unknown status",
			setup:   func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) },
			attempt: Attempt{Number: 1}, next: Status("bogus"), wantErr: ErrInvalid,
		},
		{
			name:    "pending without next time",
			setup:   func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) },
			attempt: Attempt{Number: 1}, next: StatusPending, wantErr: ErrInvalid,
		},
		{
			name:    "wrong attempt number",
			setup:   func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) },
			attempt: Attempt{Number: 2}, next: StatusSucceeded, wantErr: ErrInvalid,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			mustEndpoint(t, s, "a")
			id := newEvent(t, s, "e1", t0)
			tt.setup(t, s, id)
			before, err := s.GetDelivery(id)
			if err != nil {
				t.Fatal(err)
			}
			inFlightBefore := fmt.Sprint(s.inFlight)

			target := id
			if tt.id != nil {
				target = tt.id(id)
			}
			err = s.Complete(target, tt.attempt, tt.next, tt.nextAt)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}

			after, err := s.GetDelivery(id)
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != before.Status || len(after.Attempts) != len(before.Attempts) ||
				fmt.Sprint(s.inFlight) != inFlightBefore {
				t.Fatalf("failed Complete changed state: before %+v %s, after %+v %v",
					before, inFlightBefore, after, s.inFlight)
			}
		})
	}
}

// TestClaimCompleteConcurrent runs several workers that claim and complete
// deliveries in parallel (each delivery retries twice, then succeeds) and
// checks that no (delivery, attempt) is claimed twice, the per-endpoint cap
// always holds, and every delivery ends succeeded with exactly 3 attempts.
func TestClaimCompleteConcurrent(t *testing.T) {
	const (
		endpoints   = 4
		events      = 50
		workers     = 8
		limit       = 2
		maxAttempts = 3
	)
	s := New()
	for range endpoints {
		mustEndpoint(t, s, "a")
	}
	for i := range events {
		if _, _, _, err := s.CreateEvent(event(fmt.Sprintf("e%d", i), "a", `{}`), t0); err != nil {
			t.Fatal(err)
		}
	}
	total := int64(endpoints * events)

	var (
		claimed   sync.Map // "deliveryID/attempt" -> struct{}
		inFlight  sync.Map // endpointID -> *atomic.Int64
		succeeded atomic.Int64
		wg        sync.WaitGroup
		errs      = make(chan error, workers*4)
	)
	counter := func(ep string) *atomic.Int64 {
		c, _ := inFlight.LoadOrStore(ep, new(atomic.Int64))
		return c.(*atomic.Int64)
	}

	for range workers {
		wg.Go(func() {
			for succeeded.Load() < total {
				jobs := s.ClaimDue(t0, limit)
				if len(jobs) == 0 {
					runtime.Gosched()
					continue
				}
				for _, j := range jobs {
					key := fmt.Sprintf("%s/%d", j.DeliveryID, j.AttemptNumber)
					if _, dup := claimed.LoadOrStore(key, struct{}{}); dup {
						errs <- fmt.Errorf("claimed twice: %s", key)
					}
					if n := counter(j.EndpointID).Add(1); n > limit {
						errs <- fmt.Errorf("endpoint %s has %d in flight, cap %d", j.EndpointID, n, limit)
					}
				}
				for _, j := range jobs {
					counter(j.EndpointID).Add(-1)
					next, nextAt := StatusPending, t0
					if j.AttemptNumber == maxAttempts {
						next, nextAt = StatusSucceeded, time.Time{}
					}
					if err := s.Complete(j.DeliveryID, Attempt{Number: j.AttemptNumber}, next, nextAt); err != nil {
						errs <- err
						continue
					}
					if next == StatusSucceeded {
						succeeded.Add(1)
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

	if len(s.pending) != 0 || len(s.inFlight) != 0 {
		t.Errorf("pending=%d inFlight=%v, want both empty", len(s.pending), s.inFlight)
	}
	for id, d := range s.deliveries {
		if d.Status != StatusSucceeded || len(d.Attempts) != maxAttempts {
			t.Errorf("delivery %s: status %q with %d attempts, want succeeded with %d", id, d.Status, len(d.Attempts), maxAttempts)
		}
	}
}

// failDelivery claims the single due delivery and completes it as failed.
func failDelivery(t *testing.T, s *Store, id string, now time.Time) {
	t.Helper()
	jobs := s.ClaimDue(now, 1)
	if len(jobs) != 1 || jobs[0].DeliveryID != id {
		t.Fatalf("claimed %v, want [%s]", claimIDs(jobs), id)
	}
	if err := s.Complete(id, Attempt{Number: jobs[0].AttemptNumber, Outcome: "fail"}, StatusFailed, time.Time{}); err != nil {
		t.Fatal(err)
	}
}

func TestRedeliverFailed(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	id := newEvent(t, s, "e1", t0)

	// Two attempts in the first cycle: one retry, then failed.
	jobs := s.ClaimDue(t0, 1)
	if jobs[0].RetryIndex != 0 {
		t.Fatalf("first attempt RetryIndex = %d, want 0", jobs[0].RetryIndex)
	}
	if err := s.Complete(id, Attempt{Number: 1}, StatusPending, t0); err != nil {
		t.Fatal(err)
	}
	jobs = s.ClaimDue(t0, 1)
	if jobs[0].RetryIndex != 1 {
		t.Fatalf("second attempt RetryIndex = %d, want 1", jobs[0].RetryIndex)
	}
	if err := s.Complete(id, Attempt{Number: 2}, StatusFailed, time.Time{}); err != nil {
		t.Fatal(err)
	}

	later := t0.Add(time.Hour)
	d, err := s.Redeliver(id, later)
	if err != nil {
		t.Fatalf("Redeliver: %v", err)
	}
	if d.Status != StatusPending || !d.NextAttemptAt.Equal(later) || len(d.Attempts) != 2 {
		t.Fatalf("after redeliver: %+v, want pending at %v with 2 attempts kept", d, later)
	}

	jobs = s.ClaimDue(later, 1)
	if len(jobs) != 1 || jobs[0].AttemptNumber != 3 || jobs[0].RetryIndex != 0 {
		t.Fatalf("jobs = %+v, want attempt 3 with RetryIndex 0 (budget reset)", jobs)
	}
	if err := s.Complete(id, Attempt{Number: 3}, StatusPending, later); err != nil {
		t.Fatal(err)
	}
	if jobs = s.ClaimDue(later, 1); jobs[0].RetryIndex != 1 {
		t.Fatalf("attempt 4 RetryIndex = %d, want 1", jobs[0].RetryIndex)
	}
}

func TestRedeliverErrors(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(t *testing.T, s *Store, id string)
		target  string // empty = the delivery under test
		wantErr error
	}{
		{"unknown", func(*testing.T, *Store, string) {}, "dlv_missing", ErrNotFound},
		{"pending", func(*testing.T, *Store, string) {}, "", ErrConflict},
		{"in_flight", func(_ *testing.T, s *Store, _ string) { s.ClaimDue(t0, 1) }, "", ErrConflict},
		{"succeeded", func(t *testing.T, s *Store, id string) {
			s.ClaimDue(t0, 1)
			if err := s.Complete(id, Attempt{Number: 1}, StatusSucceeded, time.Time{}); err != nil {
				t.Fatal(err)
			}
		}, "", ErrConflict},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := New()
			mustEndpoint(t, s, "a")
			id := newEvent(t, s, "e1", t0)
			tt.setup(t, s, id)
			before, err := s.GetDelivery(id)
			if err != nil {
				t.Fatal(err)
			}
			pendingBefore := len(s.pending)

			target := tt.target
			if target == "" {
				target = id
			}
			if _, err := s.Redeliver(target, t0.Add(time.Hour)); !errors.Is(err, tt.wantErr) {
				t.Fatalf("err = %v, want %v", err, tt.wantErr)
			}
			after, err := s.GetDelivery(id)
			if err != nil {
				t.Fatal(err)
			}
			if after.Status != before.Status || !after.NextAttemptAt.Equal(before.NextAttemptAt) || len(s.pending) != pendingBefore {
				t.Fatalf("failed Redeliver changed state: before %+v, after %+v", before, after)
			}
		})
	}
}

func TestRedeliverConcurrent(t *testing.T) {
	s := New()
	mustEndpoint(t, s, "a")
	id := newEvent(t, s, "e1", t0)
	failDelivery(t, s, id, t0)

	const n = 20
	var wg sync.WaitGroup
	var ok, conflict atomic.Int64
	errs := make(chan error, n)
	for range n {
		wg.Go(func() {
			_, err := s.Redeliver(id, t0)
			switch {
			case err == nil:
				ok.Add(1)
			case errors.Is(err, ErrConflict):
				conflict.Add(1)
			default:
				errs <- err
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	if ok.Load() != 1 || conflict.Load() != n-1 {
		t.Fatalf("ok=%d conflict=%d, want exactly one redeliver to win", ok.Load(), conflict.Load())
	}
	if jobs := s.ClaimDue(t0, 10); len(jobs) != 1 {
		t.Fatalf("claimed %d, want the delivery exactly once", len(jobs))
	}
}
