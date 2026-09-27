package dispatch

import (
	"context"
	"errors"
	"net"
	"net/url"
	"syscall"
	"testing"
)

func TestClassify(t *testing.T) {
	tests := []struct {
		name   string
		status int
		err    error
		want   Outcome
	}{
		{"200", 200, nil, OutcomeSuccess},
		{"201", 201, nil, OutcomeSuccess},
		{"204", 204, nil, OutcomeSuccess},
		{"299", 299, nil, OutcomeSuccess},
		{"101 informational", 101, nil, OutcomeFail},
		{"199", 199, nil, OutcomeFail},
		{"300", 300, nil, OutcomeFail},
		{"301 redirect not followed", 301, nil, OutcomeFail},
		{"304", 304, nil, OutcomeFail},
		{"400", 400, nil, OutcomeFail},
		{"401", 401, nil, OutcomeFail},
		{"404", 404, nil, OutcomeFail},
		{"410", 410, nil, OutcomeFail},
		{"422", 422, nil, OutcomeFail},
		{"408 request timeout", 408, nil, OutcomeRetry},
		{"429 too many requests", 429, nil, OutcomeRetry},
		{"500", 500, nil, OutcomeRetry},
		{"502", 502, nil, OutcomeRetry},
		{"503", 503, nil, OutcomeRetry},
		{"504", 504, nil, OutcomeRetry},
		{"599", 599, nil, OutcomeRetry},
		{"600 out of range", 600, nil, OutcomeFail},
		{"deadline exceeded", 0, context.DeadlineExceeded, OutcomeRetry},
		{"url timeout", 0, &url.Error{Op: "Post", URL: "http://x", Err: context.DeadlineExceeded}, OutcomeRetry},
		{"connection refused", 0, &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, OutcomeRetry},
		{"dns error", 0, &net.DNSError{Err: "no such host", Name: "x.invalid", IsNotFound: true}, OutcomeRetry},
		{"generic error", 0, errors.New("boom"), OutcomeRetry},
		{"error wins over 2xx status", 200, errors.New("body read failed"), OutcomeRetry},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Classify(tt.status, tt.err); got != tt.want {
				t.Fatalf("Classify(%d, %v) = %s, want %s", tt.status, tt.err, got, tt.want)
			}
		})
	}
}
