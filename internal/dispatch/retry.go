package dispatch

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// RetryPolicy decides the delay before each retry.
type RetryPolicy struct {
	// Schedule[i] is the base delay before retry i; len(Schedule) is the
	// maximum number of retries, so a delivery gets 1+len(Schedule) attempts.
	Schedule []time.Duration
	// Jitter randomizes a base delay. Nil means no jitter.
	Jitter func(time.Duration) time.Duration
	// MaxRetryAfter caps an honored Retry-After. Zero ignores Retry-After.
	MaxRetryAfter time.Duration
}

// DefaultRetryPolicy is roughly exponential over ~24h with ±20% jitter.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		Schedule: []time.Duration{
			time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute,
			16 * time.Minute, 32 * time.Minute, time.Hour, 2 * time.Hour,
			4 * time.Hour, 8 * time.Hour, 8 * time.Hour,
		},
		Jitter:        ProportionalJitter(0.2, rand.Float64),
		MaxRetryAfter: 8 * time.Hour,
	}
}

// ProportionalJitter returns a jitter that scales d uniformly into
// [d*(1-fraction), d*(1+fraction)). rnd must return values in [0, 1).
func ProportionalJitter(fraction float64, rnd func() float64) func(time.Duration) time.Duration {
	return func(d time.Duration) time.Duration {
		return time.Duration(float64(d) * (1 - fraction + 2*fraction*rnd()))
	}
}

// NextDelay returns the delay before retry retryIndex (0-based: the first
// retry after the first failed attempt is 0), or false when the schedule is
// exhausted. A Retry-After (capped at MaxRetryAfter) can only lengthen the
// jittered delay, never shorten it.
func (p RetryPolicy) NextDelay(retryIndex int, retryAfter time.Duration) (time.Duration, bool) {
	if retryIndex < 0 || retryIndex >= len(p.Schedule) {
		return 0, false
	}
	delay := p.Schedule[retryIndex]
	if p.Jitter != nil {
		delay = p.Jitter(delay)
	}
	return max(delay, min(retryAfter, p.MaxRetryAfter)), true
}

// ParseRetryAfter parses a Retry-After header in delay-seconds or HTTP-date
// form. A date in the past yields 0. It reports false for an empty or
// malformed value.
func ParseRetryAfter(value string, now time.Time) (time.Duration, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if secs, err := strconv.ParseInt(value, 10, 64); err == nil {
		if secs < 0 {
			return 0, false
		}
		if secs > int64(math.MaxInt64/time.Second) {
			return time.Duration(math.MaxInt64), true
		}
		return time.Duration(secs) * time.Second, true
	}
	at, err := http.ParseTime(value)
	if err != nil {
		return 0, false
	}
	return max(at.Sub(now), 0), true
}

// ParseSchedule parses a comma-separated list of positive durations,
// e.g. "1s,2s,4s".
func ParseSchedule(s string) ([]time.Duration, error) {
	if strings.TrimSpace(s) == "" {
		return nil, errors.New("parse schedule: empty")
	}
	parts := strings.Split(s, ",")
	schedule := make([]time.Duration, 0, len(parts))
	for i, part := range parts {
		d, err := time.ParseDuration(strings.TrimSpace(part))
		if err != nil {
			return nil, fmt.Errorf("parse schedule: entry %d: %w", i, err)
		}
		if d <= 0 {
			return nil, fmt.Errorf("parse schedule: entry %d: %s is not positive", i, d)
		}
		schedule = append(schedule, d)
	}
	return schedule, nil
}
