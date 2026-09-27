# Implementation Plan

Build order; each task ships with its tests and passes `go test ./...` (and `-race` where noted).

- [ ] **1. Store: types, endpoints, events with atomic fan-out**
  - Files: `go.mod`, `internal/store/store.go`, `internal/store/store_test.go`
  - Proves it: table tests for `CreateEndpoint`; `CreateEvent` fans out only to subscribed endpoints, zero-subscriber event, identical replay → not created, conflicting `event_id` → error; getters return deep copies (mutating result doesn't affect store).

- [ ] **2. Store: `ClaimDue` / `Complete` state machine**
  - Files: `internal/store/store.go`, `internal/store/claim_test.go`
  - Proves it: claims only `next_attempt_at <= now`, oldest first, respects per-endpoint cap; never double-claims; `Complete` appends attempt, sets next state, frees slot, errors if not `in_flight`; concurrent claim/complete test under `-race` (each delivery claimed once per due time).

- [ ] **3. Signing, outcome classification, retry policy**
  - Files: `internal/dispatch/sign.go`, `internal/dispatch/classify.go`, `internal/dispatch/retry.go`, `internal/dispatch/{sign,classify,retry}_test.go`
  - Proves it: `Sign` known vector; `Classify` table (2xx, 3xx, 4xx, 408, 429, 5xx, timeout, conn error); `NextDelay` table (each index, exhaustion, jitter bounds, Retry-After seconds/date/invalid/over cap); `ParseSchedule("1s,2s")` valid/invalid.

- [ ] **4. Dispatcher: `DispatchDue`, sender, `Run` with injected clock**
  - Files: `internal/dispatch/dispatcher.go`, `internal/dispatch/dispatcher_test.go`
  - Proves it: fake clock + `httptest`: success; 500 → retry at `now+schedule[0]`, advance → retried, exhausted → `failed` with 1+len(schedule) attempts; 400 → failed after 1; 429 Retry-After honored; short timeout → retry; merchant verifies signature over received bytes + headers; **isolation test** (blocked endpoint A, fast B all succeed); `Run` stops on cancel and waits for in-flight. Run with `-race`.

- [ ] **5. HTTP API: endpoints, events, event/delivery views, healthz**
  - Files: `internal/api/api.go`, `internal/api/api_test.go`
  - Proves it: table tests per route for every status code in design §3 (201/202/200/400/404/409/413); secret never in responses; response field sets match design.

- [ ] **6. `main.go`: config, wiring, graceful shutdown**
  - Files: `main.go`, `main_test.go`
  - Proves it: config parsing (`PORT`, `RETRY_SCHEDULE` valid/invalid); end-to-end race test: real server + `Run`, concurrent `POST /events` to several `httptest` merchants → every delivery `succeeded`, merchant hits ≥ deliveries. Run with `-race`.

- [ ] **7. Redeliver (nice to have)**
  - Files: `internal/store/store.go`, `internal/api/api.go`, `internal/store/claim_test.go`, `internal/api/api_test.go`
  - Proves it: `failed` → `pending` with `next_attempt_at=now`, retry budget reset, history kept; 404 unknown id; 409 when not `failed`; dispatcher picks it up on next tick.
