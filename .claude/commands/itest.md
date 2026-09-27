---
description: Write black-box integration tests from spec/design.md and run them against the service in Docker
argument-hint: [optional focus, e.g. scenarios or guarantees to prioritise]
allowed-tools: Bash(gofmt:*), Bash(go vet:*), Bash(go test:*), Bash(make:*), Bash(docker:*)
---

Write integration tests in integration/integration_test.go
(`//go:build integration`, package `integration`). The Makefile, Dockerfile
and compose.yaml already exist: do not modify them or any application code.
Extra focus: $ARGUMENTS

## Derive the tests from spec/design.md
Read these sections first: Interfaces (endpoints and error cases),
Invariants, Edge cases, Concurrency, Test plan. Then cover:
- The happy path of each endpoint: status code and the exact set of
  response fields.
- Each guarantee or invariant observable over HTTP. Show concurrency
  guarantees with about 20 parallel requests, then check the final state.
- At least one case per error status code in the design. Unit tests cover
  the full error table, so don't repeat all of it.
- Anything in the design that can't be checked over HTTP: list it in the
  summary. Don't invent behaviour the design doesn't state.

## How to write them
- Target BASE_URL (default http://localhost:8080). Standard library only,
  never import internal/, pass a context to every request.
- Before testing, poll a cheap endpoint from the design until the service
  answers (timeout ~30s). Distroless has no shell, so there is no compose
  healthcheck.
- Make them safe to re-run against a service that already has data:
  - Build unique values (e.g. idempotency keys, IDs) from
    time.Now().UnixNano() plus a scenario name.
  - Assert changes in state, not absolute values.
  - When a scenario needs an exact starting state, create it through the
    API first. If that's impossible, t.Skip with the reason.
- Never call t.Fatal from goroutines: collect results, then assert.

## Verify
1. gofmt -l . , go vet -tags=integration ./... , go vet ./... , go test ./...
   (the last two confirm the tagged package doesn't break normal builds).
2. If Docker is running, run `make test-integration`. If it isn't, say so
   plainly and don't report the integration tests as verified.

## Summarise
- Which design guarantees the tests cover, and which can't be checked
  over HTTP.
- Results of each command you ran.
- Any risk or flakiness you noticed.