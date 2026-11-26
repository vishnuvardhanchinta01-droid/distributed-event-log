#!/usr/bin/env bash
# Usage: scripts/cluster.sh start | stop | kill <id>
set -euo pipefail            # stop on any error
cd "$(dirname "$0")/.."      # always run from the project root

PIDDIR=data/pids
LOGDIR=data/logs

start() {
  mkdir -p "$PIDDIR" "$LOGDIR"
  go build -o bin/broker ./cmd/broker

  # refuse to start if an old broker is still holding a port
  for id in 1 2 3; do
    port=$((9000 + id))
    if nc -z localhost "$port" 2>/dev/null; then
      echo "port $port is already in use (an old broker still running?)"
      echo "fix: pkill -f bin/broker   then: rm -f $PIDDIR/*.pid"
      exit 1
    fi
  done

  for id in 1 2 3; do
    port=$((9000 + id))
    ./bin/broker --id "$id" --port "$port" --data "data/broker-$id" \
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
  # TODO (you): 
  #   1. if the pid file doesn't exist: echo "broker $id is not running" and return
  #      (hint: if [ ! -f "$pidfile" ]; then ... fi)
  if [ ! -f "$pidfile" ]; 
  then echo "broker $id is not running"
  return
  fi
  #   2. kill the PID stored in the file   (hint: kill "$(cat "$pidfile")")
  kill "$(cat "$pidfile")" 2>/dev/null || echo "broker $id was already dead"
  #   3. delete the pid file               (hint: rm -f)
  rm -f "$pidfile"
  #   4. echo "stopped broker $id"
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
  *)     echo "usage: $0 start | stop | kill <id>"; exit 1 ;;
esac