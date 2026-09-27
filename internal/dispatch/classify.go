package dispatch

import "net/http"

// Outcome is the result of one delivery attempt.
type Outcome string

const (
	OutcomeSuccess Outcome = "success"
	OutcomeRetry   Outcome = "retry"
	OutcomeFail    Outcome = "fail"
)

// Classify maps an attempt result to an outcome. Any transport error (timeout,
// connection refused, DNS) is retried; with a response, 2xx succeeds, 408, 429
// and 5xx are retried, and everything else (1xx, 3xx, other 4xx) fails.
func Classify(statusCode int, err error) Outcome {
	switch {
	case err != nil:
		return OutcomeRetry
	case statusCode >= 200 && statusCode <= 299:
		return OutcomeSuccess
	case statusCode == http.StatusRequestTimeout,
		statusCode == http.StatusTooManyRequests,
		statusCode >= 500 && statusCode <= 599:
		return OutcomeRetry
	default:
		return OutcomeFail
	}
}
