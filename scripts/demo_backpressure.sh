#!/bin/bash
# the same flood against a slow broker, first WITHOUT a limit, then WITH one.
# One broker on its own (no ZooKeeper, no followers), every write takes 5ms ("slow disk").
#   ./scripts/demo_backpressure.sh            (LIMIT=8 PRODUCERS=100 by default)
set -e
cd "$(dirname "$0")/.."
LIMIT=${LIMIT:-8}
PRODUCERS=${PRODUCERS:-100}
PORT=9101
mkdir -p data
go build -o bin/broker ./cmd/broker
go build -o bin/flood ./cmd/flood

run() { # $1 = max-inflight (0 = no limit)
  DIR=data/demo-bp-$1
  rm -rf "$DIR"
  ./bin/broker --id 9 --port $PORT --data "$DIR" --partitions 1 --slow-disk 5ms --max-inflight "$1" >"data/demo-bp-$1.log" 2>&1 &
  PID=$!
  for _ in $(seq 50); do nc -z localhost $PORT 2>/dev/null && break; sleep 0.1; done
  ./bin/flood --addr localhost:$PORT --producers "$PRODUCERS" --msgs 10 --timeout 300ms --max-time 15s || true
  if kill $PID 2>/dev/null; then wait $PID 2>/dev/null || true; else echo "  !! the broker had already DIED. Last lines of its log:"; tail -3 "data/demo-bp-$1.log"; fi
}

echo "=================  ROUND 1: no limit (everyone waits in one long line)  ================="
run 0
echo
echo "=================  ROUND 2: --max-inflight $LIMIT (extra orders get BUSY)  ================="
run "$LIMIT"
