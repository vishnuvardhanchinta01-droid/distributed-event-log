#!/usr/bin/env bash
# Usage: scripts/cluster.sh start | stop | kill <id>
set -euo pipefail            # stop on any error
cd "$(dirname "$0")/.."      # always run from the project root

PIDDIR=data/pids
ZK=${ZK:-localhost:2181}   # the ZooKeeper "notice board"
LOGDIR=data/logs

start() {
  if ! nc -z ${ZK%:*} ${ZK#*:} 2>/dev/null; then
    echo "ZooKeeper is not running on $ZK. Start it first: zkServer start"
    exit 1
  fi
  mkdir -p "$PIDDIR" "$LOGDIR"
  go build -o bin/broker ./cmd/broker

  # refuse to start if an old broker is still holding a port
  for id in 1 2 3; do
    port=$((9000 + id))
    if nc -z localhost "$port" 2>/dev/null; then
      echo "port $port is already in use (an old broker still running?)"
      echo "fix: ./scripts/cluster.sh stop   (or: pkill -f \"bin/broker --id\"; rm -f $PIDDIR/*.pid)"
      exit 1
    fi
  done

  for id in 1 2 3; do
    port=$((9000 + id))
    # no hard-coded leader any more. Every broker races for the chair in ZooKeeper,
    # and whoever wins uses this acks mode (ACKS=1 ./scripts/cluster.sh start).
    extra="--acks ${ACKS:-all}"
    ./bin/broker --id "$id" --port "$port" --data "data/broker-$id" --zk "$ZK" $extra \
      > "$LOGDIR/broker-$id.log" 2>&1 &
    echo $! > "$PIDDIR/broker-$id.pid"
    echo "started broker $id on :$port (pid $!)"
  done

  # "process started" is not the same as "ready to accept connections":
  # wait until each port actually answers (up to ~5 seconds).
  for id in 1 2 3; do
    port=$((9000 + id))
    tries=0
    until nc -z localhost "$port" 2>/dev/null; do
      tries=$((tries + 1))
      if [ "$tries" -gt 50 ]; then
        echo "broker $id did not become ready; see $LOGDIR/broker-$id.log"
        exit 1
      fi
      sleep 0.1
    done
  done
  echo "all brokers ready"
}

kill_one() {
  id=$1
  pidfile="$PIDDIR/broker-$id.pid"
  if [ ! -f "$pidfile" ]; 
  then echo "broker $id is not running"
  return
  fi
  kill "$(cat "$pidfile")" 2>/dev/null || echo "broker $id was already dead"
  rm -f "$pidfile"
  echo "stopped broker $id"
}

stop() {
  for id in 1 2 3; do
    kill_one "$id"
  done
}

case "${1:-}" in
  start) start ;;
  stop)  stop ;;
  kill)  kill_one "${2:?usage: cluster.sh kill <id>}" ;;
  board) go run ./cmd/board --zk "$ZK" ;;   # who is alive, who is leader?
  *)     echo "usage: $0 start | stop | kill <id> | board"; exit 1 ;;
esac