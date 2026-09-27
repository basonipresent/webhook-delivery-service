---
description: Generate scripts/demo.sh that exercises the running service end to end, based on spec/design.md
argument-hint: [optional extra scenarios to include]
---

Read spec/design.md and the implemented HTTP handlers, then write
scripts/demo.sh: a bash script that demonstrates the service against a
running instance.

Requirements:
- Use BASE_URL (default http://localhost:8080) and curl only.
- Scenarios, each with a clear printed heading:
  1. Happy path for each main endpoint.
  2. Each validation error from the design (show status code + body).
  3. Any guarantee from the design demonstrated under concurrency
     (e.g. parallel requests with `&` and `wait`), then print the final
     state that proves the guarantee held.
  4. Extra scenarios: $ARGUMENTS
- Print the HTTP status code for every request.
- Fail fast with a clear message if the service is not reachable.
- Keep it short and readable; no dependencies beyond bash and curl.

Only write scripts/demo.sh. Do not modify application code.
Then run `chmod +x scripts/demo.sh` and tell me how to run it.