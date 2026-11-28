#!/usr/bin/env bash
# Demo: the ZOMBIE LEADER.
# We FREEZE the leader (kill -STOP: like a long GC pause or a network cut), ZooKeeper gives up on it,
# a new leader is elected... then we UNFREEZE it (kill -CONT). Does it still take writes?
cd "$(dirname "$0")/.."
step()  { echo; echo "==================== $1"; }
board() { ./bin/board | sed 's/^/   /'; }
field() { ./bin/board | awk -v k="$1" '$0 ~ "^"k {print $NF}'; }
notebook() {   # every partition of broker $1, one line each: "p0: apple cherry"
  echo "   📒 broker $1:"
  for d in data/broker-$1/p*; do echo "      $(basename "$d"): $(strings "$d/00000000.log" 2>/dev/null | tr '\n' ' ')"; done
}

go build -o bin/producer ./cmd/producer && go build -o bin/board ./cmd/board || exit 1

step "0. Fresh start with ACKS=1, write apple and banana"
./scripts/cluster.sh stop > /dev/null 2>&1
rm -rf data/broker-1 data/broker-2 data/broker-3
./bin/board --reset-isr > /dev/null
ACKS=1 ./scripts/cluster.sh start > /dev/null || exit 1
sleep 1
./bin/producer apple banana | sed 's/^/   /'
L=$(field leader)
board

step "1. FREEZE the leader (broker $L). It can't run any code, not even its heartbeat."
kill -STOP "$(cat data/pids/broker-$L.pid)"
echo -n "   waiting for ZooKeeper to give up on it and elect someone else"
for t in $(seq 1 40); do
  N=$(field leader)
  [ -n "$N" ] && [ "$N" != "$L" ] && [ "$N" != "(nobody)" ] && break
  echo -n "."; sleep 0.5
done
echo; board

step "2. The producer asks ZooKeeper and writes cherry to the NEW leader ($N)"
./bin/producer cherry | sed 's/^/   /'

step "3. UNFREEZE broker $L. It wakes up still believing it's the leader..."
kill -CONT "$(cat data/pids/broker-$L.pid)"
sleep 2
echo "   An old client that still remembers broker $L writes 'zombie' straight to it:"
echo "   (if it refuses, the producer retries for ~10 s, then gives up)"
./bin/producer --addr "localhost:900$L" zombie 2>&1 | tail -1 | sed 's/^/   /'

step "4. Compare the notebooks: is there one truth, or two?"
notebook "$L"
notebook "$N"
grep -h "EXPIRED\|stepping down\|new session" "data/logs/broker-$L.log" | sed 's/^/   log of '"$L"': /'

step "5. Stop everything"
./scripts/cluster.sh stop
