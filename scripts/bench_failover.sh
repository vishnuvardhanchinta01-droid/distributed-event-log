#!/usr/bin/env bash
# Benchmark: how long can NOBODY write when the leader dies?
# Writes a message every 20ms for 15s, kills the leader 5s in, reports the longest gap.
# Twice: a clean stop (broker says goodbye to ZooKeeper) and kill -9 (ZooKeeper waits for the session timeout).
# Needs ZooKeeper. Appends to bench/results.md.
cd "$(dirname "$0")/.."
OUT=bench/results.md
mkdir -p bench
go build -o bin/bench ./cmd/bench && go build -o bin/board ./cmd/board || exit 1

[ -f "$OUT" ] || echo "# Benchmarks" > "$OUT"
{ echo; echo "## Failover: longest time without a successful write"; echo; } >> "$OUT"

for how in stop kill9; do
  ./scripts/cluster.sh stop > /dev/null 2>&1
  rm -rf data/broker-1 data/broker-2 data/broker-3
  ./bin/board --reset-isr > /dev/null
  ./scripts/cluster.sh start > /dev/null || exit 1
  sleep 2
  echo "== leader dies by: $how"
  ./bin/bench --failover 15s > data/logs/bench-failover.txt 2>&1 &
  B=$!
  sleep 5
  L=$(./bin/board | awk '/^leader:/ {print $2}')
  if [ "$how" = kill9 ]; then
    echo "   💥 kill -9 leader (broker $L)"
    kill -9 "$(cat data/pids/broker-$L.pid)"; rm -f "data/pids/broker-$L.pid"
  else
    echo "   ✋ clean stop of leader (broker $L)"
    ./scripts/cluster.sh kill "$L" > /dev/null
  fi
  wait $B
  sed 's/^/   /' data/logs/bench-failover.txt
  echo "- **$how**: $(grep LONGEST data/logs/bench-failover.txt)" >> "$OUT"
done
./scripts/cluster.sh stop > /dev/null
echo; echo "saved to $OUT"
