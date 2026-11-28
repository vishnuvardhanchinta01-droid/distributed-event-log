#!/usr/bin/env bash
# Demo: a consumer CRASHES in the middle of a batch.
# Ann is fast. Bob is slow (1 s per page) and commits his bookmark only after a whole batch.
# We kill -9 Bob halfway through: what does Ann do with Bob's notebook?
cd "$(dirname "$0")/.."
step() { echo; echo "==================== $1"; }
G="demo$(date +%s)"   # a brand-new team, so no old bookmarks

go build -o bin/producer ./cmd/producer && go build -o bin/board ./cmd/board && go build -o bin/consumer ./cmd/consumer || exit 1

step "0. Fresh cluster"
./scripts/cluster.sh stop > /dev/null 2>&1
rm -rf data/broker-1 data/broker-2 data/broker-3
./bin/board --reset-isr > /dev/null
./scripts/cluster.sh start > /dev/null || exit 1
sleep 1

step "1. Ann (fast) and Bob (slow: 1 s per page) join team $G"
./bin/consumer --group "$G" --id ann > data/logs/consumer-ann.log 2>&1 &
ANN=$!
sleep 1
./bin/consumer --group "$G" --id bob --slow 1s > data/logs/consumer-bob.log 2>&1 &
BOB=$!
sleep 2
grep -h REBALANCE data/logs/consumer-ann.log data/logs/consumer-bob.log | tail -2 | sed 's/^/   /'

step "2. Produce 12 messages"
./bin/producer k1:a k2:b k3:c k4:d k5:e k6:f k7:g k8:h k9:i k10:j k11:k k12:l > /dev/null || exit 1
./bin/board | grep "group $G" | sed 's/^/   /'

step "3. 3 seconds later: 💥 kill -9 Bob in the middle of his batch"
sleep 3
kill -9 $BOB
echo "   Bob had processed:"; grep "offset" data/logs/consumer-bob.log | sed 's/^/      /'
echo "   ...but his bookmark was never saved (he commits after the WHOLE batch)"

step "4. ZooKeeper notices Bob is gone (~4-6 s), the team rebalances, Ann takes over"
sleep 8
grep -h "REBALANCE\|starting at" data/logs/consumer-ann.log | tail -4 | sed 's/^/   /'

step "5. Pages that were processed TWICE (by Bob, then by Ann)"
grep -h "offset" data/logs/consumer-ann.log data/logs/consumer-bob.log | sed 's/^\[[a-z]*\] //' | sort | uniq -d | sed 's/^/   🔁 /'
./bin/board | grep "group $G" | sed 's/^/   /'

step "6. Stop everything"
kill $ANN 2>/dev/null
./scripts/cluster.sh stop
