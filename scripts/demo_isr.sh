#!/usr/bin/env bash
# Demo: a follower that falls behind leaves the ISR,
# can NOT become leader while behind, and rejoins after catching up.
cd "$(dirname "$0")/.."
step()  { echo; echo "==================== $1"; }
board() { ./bin/board | sed 's/^/   /'; }
field() { ./bin/board | awk -v k="$1" '$0 ~ "^"k {print $NF}'; }

go build -o bin/producer ./cmd/producer && go build -o bin/board ./cmd/board || exit 1

step "0. Fresh start + one write"
./scripts/cluster.sh stop > /dev/null 2>&1
rm -rf data/broker-1 data/broker-2 data/broker-3
./bin/board --reset-isr > /dev/null
./scripts/cluster.sh start > /dev/null || exit 1
sleep 1
./bin/producer apple | sed 's/^/   /'
board
L=$(field leader)
F=""; O=""
for i in 1 2 3; do
  [ "$i" = "$L" ] && continue
  if [ -z "$F" ]; then F=$i; else O=$i; fi
done
echo "   → leader is $L. We'll crash follower $F; follower $O stays healthy."

step "1. Crash follower $F, then send banana and cherry"
kill -9 "$(cat data/pids/broker-$F.pid)"; rm -f "data/pids/broker-$F.pid"
./bin/producer banana cherry 2>&1 | sed 's/^/   /'
board

step "2. Restart follower $F: it's BEHIND (missed banana, cherry), so it stays OUT of the ISR"
./bin/broker --id "$F" --port "900$F" --data "data/broker-$F" --zk localhost:2181 --acks all \
  >> "data/logs/broker-$F.log" 2>&1 &
echo $! > "data/pids/broker-$F.pid"
# wait until it is on the board (it may first wait for ZooKeeper to expire its crashed session)
for t in $(seq 1 30); do ./bin/board | grep -q "^   $F " && break; sleep 0.5; done
board

step "3. Kill the LEADER ($L). Who may take over? Only the in-sync one: $O"
./scripts/cluster.sh kill "$L" > /dev/null
sleep 2
board
grep -h "not in the ISR" "data/logs/broker-$F.log" | tail -1 | sed 's/^/   log of '"$F"': /'

step "4. Send date: the new leader catches $F up, and $F rejoins the ISR"
./bin/producer date 2>&1 | sed 's/^/   /'
./bin/producer egg 2>&1 | sed 's/^/   /'
board
echo "   follower $F's notebooks:"
for d in data/broker-$F/p*; do echo "      $(basename "$d"): $(strings "$d/00000000.log" 2>/dev/null | tr '\n' ' ')"; done

step "5. Stop everything"
./scripts/cluster.sh stop
