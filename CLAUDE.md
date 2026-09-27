# Project Guidelines

## Workflow
- The spec/ directory is the source of truth: brief.md → design.md → implementation.md.
- Follow spec/design.md. If you need to deviate, stop and explain why instead of deviating silently.
- Work on one task at a time. Stop after each task so I can review before continuing.
- If a requirement is ambiguous, ask me instead of assuming.

## Code Style (Go)
- Idiomatic Go: gofmt, clear names, small functions, early returns.
- Always handle errors; wrap them with context (fmt.Errorf("...: %w", err)). Never ignore an error.
- Pass context.Context as the first parameter for I/O and long-running operations.
- Prefer the standard library. Do not add dependencies without asking me first.
- No global mutable state. Make shared state explicit and safe for concurrent use.

## Testing
- Every task ships with tests. Use table-driven tests where they fit.
- Cover edge cases and error paths, not only the happy path.
- Add concurrency tests when shared state is involved, and run them with -race.
- Run `go test ./...` before reporting a task as done. Report failures honestly.

## Scope
- Keep changes minimal and focused on the current task.
- Do not add features, abstractions, or layers the spec doesn't require.
- Do not refactor unrelated code.

## Communication
- After each task, summarize: what changed, which files, test results, and any deviation from the design.
- Be concise. Flag risks and trade-offs explicitly.