Read spec/brief.md and write spec/design.md. Do not write any code yet.

Keep it concise (aim for one screen, bullets over prose). Cover:

1. Approach overview: the core idea in 2-3 sentences.
2. Components and data structures / data model, with key types.
3. Interfaces: API endpoints, function signatures, or CLI, whichever
   the brief implies, including error cases.
4. Key decisions, each with: the choice, the main alternative, and why.
   Include complexity (time/space) where it matters.
5. Invariants and correctness rules the solution must never violate.
6. Edge cases and failure modes, and how each is handled.
7. Only if relevant to this brief: concurrency, consistency,
   performance, or scalability concerns. Skip what doesn't apply.
8. Test plan: the critical tests that prove correctness.
9. Assumptions and open questions: list anything the brief doesn't
   specify instead of silently deciding it.

Additional focus for this problem: $ARGUMENTS

Prefer the simplest design that satisfies the brief. Do not add
features, layers, or dependencies the brief doesn't require.