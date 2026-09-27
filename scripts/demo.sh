#!/usr/bin/env bash
# End-to-end demo of the webhook delivery service against a running instance.
#
#   make docker-up && make demo          (or: BASE_URL=http://host:port scripts/demo.sh)
#
# With curl as the only tool, the demo uses merchant endpoints the *service*
# can reach on its own:
#   ok       SELF_URL/events   A delivery body has the same shape as an event,
#                              so the service re-posting it is an identical
#                              replay -> 200 (success).
#   rejects  SELF_URL/nope     404 -> permanent failure, no retry.
#   refused  localhost:1       connection refused -> retried later.
#   hangs    10.255.255.1      unroutable -> hangs until the 10s attempt timeout.
# SELF_URL is how the service reaches itself (default http://localhost:8080,
# i.e. inside its own container). Retries use the service's RETRY_SCHEDULE.
set -euo pipefail

BASE_URL="${BASE_URL:-http://localhost:8080}"
SELF_URL="${SELF_URL:-http://localhost:8080}"
RUN="${EPOCHSECONDS:-0}${RANDOM}"   # makes IDs and event types unique per run
SECRET="demo-secret-0123456789"
PARALLEL=20

heading() { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }
note()    { printf '   # %s\n' "$*"; }

# req METHOD PATH [JSON]: print the request, status code and body; set CODE/BODY.
req() {
  local method=$1 path=$2 data=${3-} out
  if [[ -n $data ]]; then
    out=$(curl -sS -X "$method" -H 'Content-Type: application/json' \
      --data-binary @- -w '\n%{http_code}' "$BASE_URL$path" <<<"$data")
  else
    out=$(curl -sS -X "$method" -w '\n%{http_code}' "$BASE_URL$path")
  fi
  out=${out//$'\r'/}   # curl on Windows writes \r\n for -w newlines
  CODE=${out##*$'\n'}
  BODY=${out%$'\n'*}
  BODY=${BODY%$'\n'}
  local shown=$BODY
  if (( ${#shown} > 400 )); then shown="${shown:0:400}..."; fi
  printf '   %-6s %-50s -> %s %s\n' "$method" "${path:0:50}" "$CODE" "$shown"
}

# field NAME: first string value of "NAME" in BODY.
field() {
  local re="\"$1\":\"([^\"]*)\""
  if [[ $BODY =~ $re ]]; then printf '%s' "${BASH_REMATCH[1]}"; return; fi
  echo "   !! no \"$1\" in: $BODY" >&2
  return 1
}

endpoint_json() { printf '{"url":"%s","event_types":["%s"],"secret":"%s"}' "$1" "$2" "$SECRET"; }
event_json()    { printf '{"event_id":"%s","type":"%s","created_at":"2026-09-27T12:00:00Z","payload":%s}' "$1" "$2" "$3"; }

# wait_until DELIVERY_ID PATTERN SECONDS: poll the delivery until its JSON contains PATTERN.
wait_until() {
  local id=$1 pattern=$2 tries=$(( $3 * 5 ))
  for (( i = 0; i < tries; i++ )); do
    [[ $(curl -sS "$BASE_URL/deliveries/$id") == *"$pattern"* ]] && return 0
    sleep 0.2
  done
  echo "   !! delivery $id: no '$pattern' after $3s" >&2
  return 1
}

# register URL TYPE: create an endpoint and print its ID.
register() { req POST /endpoints "$(endpoint_json "$1" "$2")"; EP_ID=$(field id); }

# post_event ID TYPE PAYLOAD: create an event; sets DLV to its first delivery ID.
post_event() { req POST /events "$(event_json "$1" "$2" "$3")"; DLV=$(field id); }

# ---------------------------------------------------------------------------
if ! curl -sf -o /dev/null --max-time 3 "$BASE_URL/healthz"; then
  echo "Service not reachable at $BASE_URL/healthz. Start it with: make docker-up" >&2
  exit 1
fi
echo "Service: $BASE_URL   (run $RUN)"

heading "1. Happy path: register, send, deliver, inspect"
T_OK="demo.order_paid.$RUN"
req GET /healthz
register "$SELF_URL/events" "$T_OK"
post_event "evt_$RUN" "$T_OK" '{"amount":42,"currency":"EUR"}'
OK_DLV=$DLV
wait_until "$OK_DLV" '"status":"succeeded"' 10
note "delivered, signed, one attempt with a 2xx:"
req GET "/deliveries/$OK_DLV"
req GET "/events/evt_$RUN"
note "identical replay of the same event_id is idempotent (200, no new delivery):"
req POST /events "$(event_json "evt_$RUN" "$T_OK" '{"amount":42,"currency":"EUR"}')"

heading "2. Failures: retry, permanent failure, redeliver"
T_RETRY="demo.retry.$RUN"
register "http://localhost:1/hook" "$T_RETRY"
post_event "evt_retry_$RUN" "$T_RETRY" '{"n":1}'
wait_until "$DLV" '"attempts":[{' 10
note "connection refused -> attempt recorded (status_code 0), retry scheduled at next_attempt_at:"
req GET "/deliveries/$DLV"

T_REJECT="demo.reject.$RUN"
register "$SELF_URL/nope" "$T_REJECT"
post_event "evt_reject_$RUN" "$T_REJECT" '{"n":2}'
REJ_DLV=$DLV
wait_until "$REJ_DLV" '"status":"failed"' 10
note "404 -> failed after one attempt, no retry:"
req GET "/deliveries/$REJ_DLV"
note "manual redeliver (DLQ-style) re-queues it with a fresh retry budget, history kept:"
req POST "/deliveries/$REJ_DLV/redeliver"
wait_until "$REJ_DLV" '"number":2' 10
req GET "/deliveries/$REJ_DLV"

heading "3. Validation errors"
req POST /endpoints '{"url":'
req POST /endpoints "$(endpoint_json "ftp://merchant.example" "x")"
req POST /endpoints '{"url":"https://merchant.example","event_types":[],"secret":"demo-secret-0123456789"}'
req POST /endpoints '{"url":"https://merchant.example","event_types":["x"],"secret":"short"}'
req POST /events "{\"event_id\":\"evt_bad_$RUN\",\"created_at\":\"2026-09-27T12:00:00Z\",\"payload\":{}}"
req POST /events "{\"event_id\":\"evt_bad_$RUN\",\"type\":\"x\",\"created_at\":\"yesterday\",\"payload\":{}}"
req POST /events "$(event_json "evt_bad_$RUN" x null)"
req POST /events "$(event_json "evt bad $RUN" x '{}')"
note "same event_id, different content:"
req POST /events "$(event_json "evt_$RUN" "$T_OK" '{"amount":99}')"
note "body over 1 MiB:"
big=x; while (( ${#big} <= 1 << 20 )); do big=$big$big; done
req POST /events "$(event_json "evt_big_$RUN" x "\"$big\"")"
unset big
req GET "/events/evt_missing_$RUN"
req GET "/deliveries/dlv_missing_$RUN"
req POST "/deliveries/dlv_missing_$RUN/redeliver"
note "only failed deliveries can be redelivered:"
req POST "/deliveries/$OK_DLV/redeliver"

heading "4a. Concurrency: $PARALLEL parallel sends of one event_id"
EV_DUP="evt_dup_$RUN"
DUP_JSON=$(event_json "$EV_DUP" "$T_OK" '{"n":3}')
codes=$(for (( i = 0; i < PARALLEL; i++ )); do
  curl -sS -o /dev/null -w '%{http_code}\n' -H 'Content-Type: application/json' \
    --data-binary "$DUP_JSON" "$BASE_URL/events" &
done; wait)
codes=${codes//$'\r'/}
n202=0; n200=0
for c in $codes; do
  case $c in 202) n202=$((n202 + 1)) ;; 200) n200=$((n200 + 1)) ;; esac
done
echo "   POST   /events x$PARALLEL -> $(echo $codes)"
req GET "/events/$EV_DUP"
dcount=0; rest=$BODY
while [[ $rest == *'"endpoint_id"'* ]]; do dcount=$((dcount + 1)); rest=${rest#*'"endpoint_id"'}; done
echo "   RESULT: $n202 x 202 (created), $n200 x 200 (replays), deliveries for the event: $dcount (want 1 x 202, 1 delivery)"

heading "4b. Isolation: a hanging endpoint must not delay a healthy one"
T_ISO="demo.isolation.$RUN"
register "http://10.255.255.1/hook" "$T_ISO"; SLOW_EP=$EP_ID
register "$SELF_URL/events" "$T_ISO"; FAST_EP=$EP_ID
note "posting $PARALLEL events in parallel, each goes to both endpoints"
codes=$(for (( i = 0; i < PARALLEL; i++ )); do
  curl -sS -o /dev/null -w '%{http_code}\n' -H 'Content-Type: application/json' \
    --data-binary "$(event_json "evt_iso_${RUN}_$i" "$T_ISO" "{\"i\":$i}")" "$BASE_URL/events" &
done; wait)
echo "   POST   /events x$PARALLEL -> $(echo $codes)"

tally() {  # count delivery statuses per endpoint across the isolation events
  fast_ok=0; slow_ok=0; slow_waiting=0
  local re='"endpoint_id":"([^"]+)","status":"([a-z_]+)"' rest
  for (( i = 0; i < PARALLEL; i++ )); do
    rest=$(curl -sS "$BASE_URL/events/evt_iso_${RUN}_$i")
    while [[ $rest =~ $re ]]; do
      case "${BASH_REMATCH[1]}:${BASH_REMATCH[2]}" in
        "$FAST_EP:succeeded") fast_ok=$((fast_ok + 1)) ;;
        "$SLOW_EP:succeeded") slow_ok=$((slow_ok + 1)) ;;
        "$SLOW_EP:"*)         slow_waiting=$((slow_waiting + 1)) ;;
      esac
      rest=${rest#*"${BASH_REMATCH[0]}"}
    done
  done
}
start=$SECONDS
for (( t = 0; t < 25; t++ )); do
  tally
  (( fast_ok == PARALLEL )) && break
  sleep 0.2
done
echo "   RESULT after ~$((SECONDS - start))s: healthy endpoint $fast_ok/$PARALLEL succeeded;" \
     "hanging endpoint $slow_ok succeeded, $slow_waiting still in_flight/pending (max 4 in flight, 10s timeout each)"
note "one of the hanging deliveries, still waiting on the dead merchant:"
req GET "/events/evt_iso_${RUN}_0"

heading "Done"
