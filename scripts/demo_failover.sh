#!/usr/bin/env bash
# Demo: kill the leader while a producer is sending.
# Run:  ./scripts/demo_failover.sh          (normal kill: goodbye to ZooKeeper, fast failover)
#       KILL9=1 ./scripts/demo_failover.sh  (crash: ZooKeeper waits for the 4s session timeout)
cd "$(dirname "$0")/.."
step() { echo; echo "==================== $1"; }

go build -o bin/producer ./cmd/producer && go build -o bin/board ./cmd/board || exit 1

step "0. Fresh start: 3 brokers race for the chair"
./scripts/cluster.sh stop > /dev/null 2>&1
rm -rf data/broker-1 data/broker-2 data/broker-3
./bin/board --reset-isr > /dev/null   # empty logs → forget the old in-sync list
./scripts/cluster.sh start || exit 1
sleep 1
./bin/board

step "1. Send msg-1 .. msg-12, one by one; the LEADER dies after msg-5"
for i in $(seq 1 12); do
  if [ "$i" = 6 ]; then
    L=$(./bin/board | awk '/^leader:/ {print $2}')
    if [ "${KILL9:-}" = 1 ]; then
      echo "   💥 kill -9 the leader (broker $L): no goodbye to ZooKeeper"
      kill -9 "$(cat data/pids/broker-$L.pid)"; rm -f "data/pids/broker-$L.pid"
    else
      echo "   💥 killing the leader (broker $L)"
      ./scripts/cluster.sh kill "$L" > /dev/null
    fi
  fi
  start=$(date +%s)
  ./bin/producer "msg-$i" 2>&1 | sed 's/^/   /'
  took=$(( $(date +%s) - start ))
  [ "$took" -ge 1 ] && echo "      (that one took ~${took}s)"
done

step "2. The board now"
./bin/board

step "3. Who became leader, and when (broker logs)"
grep -h "LEADER" data/logs/*.log | sed 's/^/   /'

step "4. Stop everything"
./scripts/cluster.sh stop
