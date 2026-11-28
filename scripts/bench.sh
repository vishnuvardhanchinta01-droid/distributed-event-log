#!/usr/bin/env bash
# Benchmark: throughput + latency for acks=1, quorum and all, on a fresh 3-broker cluster each time.
# Needs ZooKeeper running (zkServer start). Results go to bench/results.md (and the screen).
#   ./scripts/bench.sh                 MSGS=4000 SIZE=100 PRODUCERS=1,4,16 by default
cd "$(dirname "$0")/.."
MSGS=${MSGS:-4000}; SIZE=${SIZE:-100}; PRODUCERS=${PRODUCERS:-1,4,16}
OUT=bench/results.md
mkdir -p bench
go build -o bin/bench ./cmd/bench && go build -o bin/board ./cmd/board || exit 1

fresh() { # $1 = acks
  ./scripts/cluster.sh stop > /dev/null 2>&1
  rm -rf data/broker-1 data/broker-2 data/broker-3
  ./bin/board --reset-isr > /dev/null
  ACKS=$1 ./scripts/cluster.sh start > /dev/null || exit 1
  sleep 2   # election + the leader connects to its followers
}

{
  echo "# Benchmarks"
  echo
  echo "Run: $(date '+%Y-%m-%d %H:%M'), $(uname -sm), $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -r)"
  echo "3 brokers + ZooKeeper on one machine (localhost), 3 partitions, $SIZE-byte messages, $MSGS messages per run, no fsync."
  echo
  echo "## Writes and reads"
  echo
} > "$OUT"

first=--header
for a in 1 quorum all; do
  echo "== acks=$a" >&2
  fresh "$a"
  ./bin/bench --label "acks=$a" --producers "$PRODUCERS" --msgs "$MSGS" --size "$SIZE" $first | tee -a "$OUT"
  first=
done
./scripts/cluster.sh stop > /dev/null
echo; echo "saved to $OUT"
