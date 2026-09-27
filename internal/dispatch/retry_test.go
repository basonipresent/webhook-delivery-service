package dispatch

import (
	"math"
	"math/rand/v2"
	"net/http"
	"slices"
	"testing"
	"time"
)

func TestNextDelay(t *testing.T) {
	p := RetryPolicy{
		Schedule:      []time.Duration{time.Second, 2 * time.Second, 4 * time.Second},
		MaxRetryAfter: 10 * time.Second,
	}
	tests := []struct {
		name       string
		index      int
		retryAfter time.Duration
		want       time.Duration
		wantOK     bool
	}{
		{"first retry", 0, 0, time.Second, true},
		{"second retry", 1, 0, 2 * time.Second, true},
		{"last retry", 2, 0, 4 * time.Second, true},
		{"exhausted", 3, 0, 0, false},
		{"far past end", 100, 0, 0, false},
		{"negative index", -1, 0, 0, false},
		{"retry-after shorter than schedule is ignored", 1, time.Second, 2 * time.Second, true},
		{"retry-after longer than schedule wins", 0, 5 * time.Second, 5 * time.Second, true},
		{"retry-after capped", 0, time.Hour, 10 * time.Second, true},
		{"retry-after does not bypass exhaustion", 3, 5 * time.Second, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := p.NextDelay(tt.index, tt.retryAfter)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("NextDelay(%d, %s) = %s, %v; want %s, %v", tt.index, tt.retryAfter, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestNextDelayCapBelowScheduleKeepsSchedule(t *testing.T) {
	p := RetryPolicy{Schedule: []time.Duration{time.Minute}, MaxRetryAfter: time.Second}
	if got, _ := p.NextDelay(0, time.Hour); got != time.Minute {
		t.Fatalf("got %s, want schedule delay 1m", got)
	}
}

func TestNextDelayZeroCapIgnoresRetryAfter(t *testing.T) {
	p := RetryPolicy{Schedule: []time.Duration{time.Second}}
	if got, _ := p.NextDelay(0, time.Hour); got != time.Second {
		t.Fatalf("got %s, want 1s", got)
	}
}

func TestNextDelayAppliesJitterToScheduleOnly(t *testing.T) {
	p := RetryPolicy{
		Schedule:      []time.Duration{10 * time.Second},
		Jitter:        func(d time.Duration) time.Duration { return d / 2 },
		MaxRetryAfter: time.Hour,
	}
	if got, _ := p.NextDelay(0, 0); got != 5*time.Second {
		t.Fatalf("jittered delay = %s, want 5s", got)
	}
	if got, _ := p.NextDelay(0, 7*time.Second); got != 7*time.Second {
		t.Fatalf("retry-after delay = %s, want 7s (not jittered)", got)
	}
}

func TestProportionalJitterBounds(t *testing.T) {
	const d = 10 * time.Second
	fixed := []struct {
		rnd  float64
		want time.Duration
	}{
		{0, 8 * time.Second},
		{0.5, 10 * time.Second},
		{0.75, 11 * time.Second},
	}
	for _, tt := range fixed {
		j := ProportionalJitter(0.2, func() float64 { return tt.rnd })
		if got := j(d); got != tt.want {
			t.Errorf("rnd=%v: got %s, want %s", tt.rnd, got, tt.want)
		}
	}

	j := ProportionalJitter(0.2, rand.New(rand.NewPCG(1, 2)).Float64)
	for range 10000 {
		if got := j(d); got < 8*time.Second || got >= 12*time.Second {
			t.Fatalf("jittered %s outside [8s, 12s)", got)
		}
	}
}

func TestDefaultRetryPolicy(t *testing.T) {
	p := DefaultRetryPolicy()
	if len(p.Schedule) != 11 {
		t.Fatalf("len(Schedule) = %d, want 11 retries", len(p.Schedule))
	}
	if !slices.IsSorted(p.Schedule) {
		t.Errorf("schedule not non-decreasing: %v", p.Schedule)
	}
	var total time.Duration
	for _, d := range p.Schedule {
		total += d
	}
	if total < 23*time.Hour || total > 25*time.Hour {
		t.Errorf("schedule totals %s, want ~24h", total)
	}
	if p.Jitter == nil || p.MaxRetryAfter != 8*time.Hour {
		t.Errorf("jitter set = %v, MaxRetryAfter = %s; want jitter and 8h", p.Jitter != nil, p.MaxRetryAfter)
	}

	// Each call returns a fresh schedule, so callers cannot mutate shared state.
	p.Schedule[0] = time.Hour
	if DefaultRetryPolicy().Schedule[0] != time.Minute {
		t.Error("DefaultRetryPolicy shares its schedule slice")
	}
}

func TestParseRetryAfter(t *testing.T) {
	now := time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		value  string
		want   time.Duration
		wantOK bool
	}{
		{"seconds", "120", 2 * time.Minute, true},
		{"zero", "0", 0, true},
		{"surrounding spaces", " 5 ", 5 * time.Second, true},
		{"huge seconds clamp instead of overflow", "99999999999999999", time.Duration(math.MaxInt64), true},
		{"http-date in future", now.Add(90 * time.Second).Format(http.TimeFormat), 90 * time.Second, true},
		{"http-date in past", now.Add(-time.Hour).Format(http.TimeFormat), 0, true},
		{"empty", "", 0, false},
		{"negative", "-1", 0, false},
		{"fractional", "1.5", 0, false},
		{"garbage", "soon", 0, false},
		{"seconds overflow int64", "99999999999999999999", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := ParseRetryAfter(tt.value, now)
			if got != tt.want || ok != tt.wantOK {
				t.Fatalf("ParseRetryAfter(%q) = %s, %v; want %s, %v", tt.value, got, ok, tt.want, tt.wantOK)
			}
		})
	}
}

func TestParseSchedule(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    []time.Duration
		wantErr bool
	}{
		{"single", "1s", []time.Duration{time.Second}, false},
		{"several", "1s,2s,4s", []time.Duration{time.Second, 2 * time.Second, 4 * time.Second}, false},
		{"spaces and mixed units", " 500ms , 1m,1h30m ", []time.Duration{500 * time.Millisecond, time.Minute, 90 * time.Minute}, false},
		{"empty", "", nil, true},
		{"blank", "   ", nil, true},
		{"empty entry", "1s,,2s", nil, true},
		{"trailing comma", "1s,", nil, true},
		{"missing unit", "5", nil, true},
		{"zero", "0s", nil, true},
		{"negative", "-1s", nil, true},
		{"garbage", "soon", nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseSchedule(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !slices.Equal(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
