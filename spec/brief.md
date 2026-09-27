# Brief: Webhook Delivery Service

## Problem
Deliver events from internal systems to merchant endpoints reliably,
even when merchant servers are slow or down, without one merchant
affecting others, and with a record of every delivery attempt.

## Requirements
- Register an endpoint: URL, subscribed event types, and a signing secret.
- Accept events from internal systems (event_id, type, created_at, payload).
- Deliver each event to every endpoint subscribed to its type.
- View delivery status and attempt history per event / per delivery.
- Guarantees:
  - A slow or failing endpoint must never delay deliveries to other merchants.
  - Every request is signed with the endpoint's secret so merchants can
    verify it came from us.
  - Every request includes event_id so merchants can deduplicate.
  - At-least-once delivery: retries may cause duplicates, never silent loss.

## Constraints
- Go, standard library preferred.
- In-memory storage, single instance.
- 10-second timeout per delivery attempt.
- 60-minute session.

## Clarifications (confirmed with interviewer)
- Success = any 2xx response.
- Retry: exponential backoff with jitter over a 24-hour window;
  schedule must be configurable (seconds for the demo).
- Retry on timeouts, connection errors, 5xx, 408, and 429
  (honor Retry-After). Other 4xx fail immediately.
- After the last retry fails, the delivery is marked failed.
- Ordering is best effort; merchants use created_at to handle it.

## Assumptions (not confirmed)
- Signature: HMAC-SHA256 over timestamp + body, sent in headers; the
  timestamp lets merchants reject replayed requests.
- Event ingestion is asynchronous: accept returns 202, delivery happens
  in the background.

## Out of scope
- Merchant dashboard, persistence, multi-instance deployment,
  strict ordering, secret rotation.

## Nice to have
- Manual redeliver endpoint for failed deliveries (DLQ-style), used
  after the merchant fixes their endpoint.