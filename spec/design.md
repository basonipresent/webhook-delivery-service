# Design: Webhook Delivery Service

## 1. Approach
- Store events, endpoints and deliveries (one per event × subscribed endpoint) in a mutex-guarded in-memory store. Every delivery has a `next_attempt_at`.
- A dispatcher loop ticks every `PollInterval`. On each tick it claims due deliveries (`next_attempt_at <= now`) and runs each attempt in its own goroutine, with a cap on in-flight attempts **per endpoint**. There is no shared worker pool.
- When an attempt fails, the dispatcher records it and moves `next_attempt_at` forward. No goroutine sleeps waiting for a retry. The clock, the retry schedule and the jitter are injected, so tests run in milliseconds.

## 2. Components and data model
Layout: `main.go` (config, wiring, graceful shutdown), `internal/store`, `internal/dispatch`, `internal/api`.

```go
// internal/store
type Endpoint struct { ID, URL, Secret string; EventTypes []string; CreatedAt time.Time } // immutable after create
type Event    struct { ID, Type string; CreatedAt, ReceivedAt time.Time; Payload json.RawMessage
                       Body []byte }  // exact bytes sent to every endpoint, built once at ingest
type Status string // "pending" | "in_flight" | "succeeded" | "failed"
type Delivery struct { ID, EventID, EndpointID string; Status Status
                       NextAttemptAt time.Time; Attempts []Attempt }
type Attempt  struct { Number int; StartedAt time.Time; Duration time.Duration
                       StatusCode int /*0 = no response*/; Error string; Outcome string /*success|retry|fail*/ }

type Store struct {
    mu         sync.Mutex
    endpoints  map[string]*Endpoint
    events     map[string]*Event
    deliveries map[string]*Delivery
    pending    map[string]*Delivery // status pending: the only set scanned per tick
    inFlight   map[string]int       // endpointID -> in-flight attempts
}
```

```go
// internal/dispatch
type Clock interface{ Now() time.Time }
type RetryPolicy struct {
    Schedule      []time.Duration                     // delay before retry i; len = max retries
    Jitter        func(time.Duration) time.Duration   // default ±20%; identity in tests
    MaxRetryAfter time.Duration                       // cap on honored Retry-After
}
type Dispatcher struct { store *store.Store; client *http.Client; clock Clock; policy RetryPolicy
                         perEndpointLimit int; attemptTimeout, pollInterval time.Duration; wg sync.WaitGroup }
```

## 3. Interfaces
HTTP API (Go 1.22+ `ServeMux` patterns, JSON). Errors are returned as `{"error": "..."}`. Request bodies are capped at 1 MiB, and larger bodies get 413.

| Method & path | Success | Errors |
|---|---|---|
| `POST /endpoints` `{url, event_types[], secret}` | 201 `{id, url, event_types, created_at}` (secret never returned) | 400: bad JSON, URL not absolute http(s), empty `event_types`, secret < 16 bytes |
| `POST /events` `{event_id, type, created_at, payload}` | 202 `{event_id, deliveries:[{id, endpoint_id, status}]}`; identical replay of an existing `event_id` → 200 with the existing event, no new deliveries | 400: missing field, `created_at` not RFC3339, payload missing or `null`, `event_id` not 1–255 visible ASCII chars (it is sent as a header); 409: same `event_id`, different content |
| `GET /events/{id}` | 200: event + deliveries with attempts | 404 |
| `GET /deliveries/{id}` | 200 `{id, event_id, endpoint_id, status, next_attempt_at, attempts[]}` | 404 |
| `POST /deliveries/{id}/redeliver` *(nice to have)* | 202: status back to `pending`, `next_attempt_at=now`, retry budget reset, history kept | 404; 409 if status is not `failed` |
| `GET /healthz` | 200 | |

Outbound request to the merchant: `POST <url>`. The body is `Event.Body`, i.e. `{"event_id","type","created_at","payload"}`. Headers:
- `Content-Type: application/json`
- `Webhook-Event-Id: <event_id>`
- `Webhook-Timestamp: <unix seconds at send time>`
- `Webhook-Signature: v1=<hex(HMAC-SHA256(secret, timestamp + "." + body))>`

Key functions:
```go
func (s *Store) CreateEndpoint(ep Endpoint) (Endpoint, error)
func (s *Store) CreateEvent(ev Event, now time.Time) (Event, []Delivery, bool /*created*/, error) // atomic fan-out
func (s *Store) ClaimDue(now time.Time, perEndpointLimit int) []Job // pending→in_flight, oldest next_attempt_at first
func (s *Store) Complete(deliveryID string, a Attempt, next Status, nextAt time.Time) error // in_flight→…
func (s *Store) Redeliver(id string, now time.Time) (Delivery, error)
func Sign(secret []byte, ts int64, body []byte) string
func Classify(statusCode int, err error) Outcome            // pure
func (p RetryPolicy) NextDelay(retryIndex int, retryAfter time.Duration) (time.Duration, bool) // false = exhausted
func (d *Dispatcher) DispatchDue(ctx context.Context) int    // one tick; tests call it directly
func (d *Dispatcher) Run(ctx context.Context) error          // ticker loop; on cancel waits for in-flight
```
`Job` holds copies of the delivery ID, attempt number, URL, secret and body. The HTTP call never touches the store.

## 4. Key decisions
- **Goroutine per attempt with a per-endpoint cap (default 4)** instead of a global worker pool. With a pool of N workers, N slow endpoints at 10 s each would starve everyone else, which is head-of-line blocking. With the cap, a slow endpoint only uses its own slots. Trade-off: total goroutines are bounded only by endpoints × cap, not globally.
- **Poll due deliveries** (`ClaimDue` scans `pending`, O(P log P) per tick) instead of a min-heap plus a timer (O(log n)). Polling is simpler, has no timer and wake-up bookkeeping, and one tick is directly testable. A heap is the upgrade path if P gets large.
- **Retry state lives in data (`next_attempt_at`)**, not in sleeping goroutines or `time.AfterFunc`. A failing endpoint then holds zero goroutines between attempts, a delivery's state can always be inspected, redeliver is just a state change, and a fake clock drives everything in tests.
- **Body serialized once at ingest.** The signature is computed per attempt over the exact `[]byte` passed to `bytes.NewReader`. Re-marshalling per attempt could change the bytes and break signature verification.
- **Fresh timestamp and signature per attempt** instead of signing once. Merchants can then apply a replay window (e.g. 5 min) even to late retries.
- **Default schedule** (≈24 h, ±20% jitter): 1m, 2m, 4m, 8m, 16m, 32m, 1h, 2h, 4h, 8h, 8h. That is 11 retries, 12 attempts in total. Env `RETRY_SCHEDULE="1s,2s,4s"` overrides it for the demo.
- **Retry-After:** the delay is `max(schedule delay, Retry-After)`, capped at `MaxRetryAfter` (default 8h). Both seconds and HTTP-date forms are parsed. It still uses up one retry.
- **HTTP client:** 10 s timeout per attempt via context, redirects not followed, response body read up to 4 KiB and then closed (the body is not stored).

## 5. Invariants
- Deliveries move only along `pending → in_flight → {pending | succeeded | failed}` and `failed → pending` (redeliver). `succeeded` is terminal.
- A delivery is claimed by at most one goroutine at a time, and only the claimer may `Complete` it. `Complete` errors if the status is not `in_flight`.
- Every attempt is appended to `Attempts` before the next state is set. No attempt goes unrecorded.
- A delivery becomes `failed` only after a non-retryable outcome or an exhausted schedule. Nothing is dropped without a recorded terminal state.
- `inFlight[endpoint] <= perEndpointLimit` always.
- No store lock is held during network I/O. Store getters return deep copies.
- The event fan-out is atomic: the 202 is returned only after all deliveries exist.

## 6. Edge cases and failure modes
- **Timeout, connection refused or DNS error:** retry, with `StatusCode=0` and the error recorded.
- **2xx:** success. **408, 429, 5xx:** retry. **Other 4xx:** fail immediately. **1xx and 3xx (redirects not followed):** fail. See open questions.
- **Merchant hangs, or drip-feeds the response body:** cut off at 10 s by the context and treated as a retry.
- **`event_id` with control characters, spaces or non-ASCII:** rejected at ingest (400). Otherwise it would be an invalid `Webhook-Event-Id` header, and every attempt would fail before reaching the network.
- **No subscribed endpoints:** the event is stored with zero deliveries and returns 202.
- **Endpoint registered after an event:** that event is not delivered to it.
- **Duplicate `event_id`:** an identical replay returns 200 (idempotent producer retries). Different content returns 409. Content is compared after `json.Compact`.
- **Shutdown:** stop claiming, let in-flight attempts finish (≤10 s) and record them, then exit. Pending deliveries are lost because there is no persistence.
- **Memory:** unbounded growth, since there is no eviction. This is acceptable for the scope.

## 7. Concurrency and isolation
- A single `sync.Mutex` guards the store. Critical sections are map operations, plus the `ClaimDue` scan.
- Isolation comes from per-endpoint caps with no shared pool. A slow endpoint's retries hold no slots while waiting. The shared `http.Transport` has no per-host connection limit, so there is no cross-host blocking.
- Worst case for one bad endpoint: 4 slots × 10 s means 0.4 attempts/s for that endpoint. Its backlog grows, but only its own.
- Ordering is best effort: due deliveries are claimed oldest `next_attempt_at` first, but up to 4 can be in flight per endpoint.

## 8. Test plan
- `Classify` table: 200/204/299, 301, 400/404, 408, 429, 500/503, and timeout/connection errors.
- `NextDelay` table: each index, exhaustion, jitter bounds, and Retry-After in seconds/date/invalid/above cap.
- `Sign`: a known vector, plus a merchant-side verification in `httptest` that recomputes the HMAC over the received body and the timestamp header.
- Store: fan-out only to subscribed endpoints, duplicate/conflicting `event_id`, `ClaimDue` respects `next_attempt_at` and the cap and never double-claims, `Complete` rejects a non-in-flight delivery.
- Dispatcher with a fake clock and `httptest`:
  - A 500 schedules a retry at `now + schedule[0]`. Advancing the clock and calling `DispatchDue` retries it. Exhausting the schedule gives `failed` with 1 + len(schedule) attempts.
  - A 400 gives `failed` after 1 attempt.
  - 429 + Retry-After is honored.
  - A short `attemptTimeout` gives a retry.
- **Isolation:** endpoint A's handler blocks until the test ends, B is fast. After many events to both, all of B's deliveries succeed within a short deadline while A's are in flight or pending.
- **Race:** concurrent `POST /events` with `Run` active, under `-race`. Every delivery ends `succeeded`, and the server hit count is ≥ the number of deliveries (duplicates allowed, no loss).
- API: each status code in the table above, and the redeliver transitions.

## 9. Assumptions (confirmed by user 2026-09-27; defaults below stand)
1. **Isolation unit:** the brief says "merchant" but has no merchant entity. Isolation is per endpoint (per registration). Is that OK?
2. **Signature scheme and async ingestion:** these come from the brief's unconfirmed assumptions. Header names and the `ts.body` format are my choice.
3. **Duplicate `event_id`:** 200 for an identical replay, 409 for a conflict. Or should it always be 409?
4. **Retry-After:** does it consume a retry, and is capping it at 8h right?
5. **1xx/3xx responses:** treated as permanent failures. Confirm.
6. **Merchant secret:** supplied by the caller, minimum 16 bytes, never returned. No endpoint update/delete (not required).
7. **Per-endpoint cap:** default 4. Is there any expectation for a global concurrency limit?
8. **Delivery guarantee:** at-least-once holds only while the process is alive. A crash loses pending deliveries (in-memory constraint).
9. **Security:** no API auth, and no SSRF protection on endpoint URLs (internal service, out of scope).
