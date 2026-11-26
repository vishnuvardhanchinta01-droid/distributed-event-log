# eventlog: progress so far

A Kafka-like distributed log in Go, built step by step to learn Go and distributed systems.

**Status (2026-11-24):** Phases 1–3 done ✅. Next: **Phase 4, replication**.

| Phase | What | Status | Time taken |
|---|---|---|---|
| 1 | Plumbing: frames, TCP broker, cluster script | ✅ | ~5 h (incl. learning Go) |
| 2 | Log: store messages on disk, crash recovery | ✅ | ~1.6 h |
| 3 | Producer: send messages, broker stores them | ✅ | ~1.2 h |
| 4 | Replication: leader copies to followers | next | ~2.5–3 h |
| 5 | ZooKeeper: who is leader, failover | | ~3 h |
| 6 | Consumer groups | | ~2 h |
| 7 | Backpressure | | ~1–1.5 h |
| 8 | Demos + benchmarks | | ~1.5 h |
| 9 | Write-up | | ~1 h |

---

## Project layout

```
eventlog/
├── cmd/
│   ├── broker/main.go      the server program (--id, --port, --data)
│   ├── ping/main.go        pings brokers 9001-9003
│   └── producer/main.go    sends messages to a broker
├── internal/
│   ├── protocol/           frames: WriteFrame / ReadFrame + message types
│   ├── broker/             Broker: accept clients, answer PING / PRODUCE
│   └── commitlog/          the on-disk log: Open / Append / Read / rebuild
├── scripts/cluster.sh      start | stop | kill <id>  (3 brokers)
├── bin/broker              built program (made by cluster.sh)
└── data/
    ├── broker-1/00000000.log   each broker's own log file
    ├── logs/                   broker output
    └── pids/                   "ticket numbers" of running brokers
```

## Everyday commands

```bash
go test -count=1 ./...                 # run all tests (add -race to check for races)
scripts/cluster.sh start               # start brokers 1,2,3 on 9001-9003
go run ./cmd/ping                      # expect 3 PONGs
go run ./cmd/producer --addr localhost:9001 apple banana
scripts/cluster.sh kill 2              # stop one broker
scripts/cluster.sh stop                # stop all
pkill -f bin/broker; rm -f data/pids/*.pid   # clean up stuck old brokers
```

---

## Phase 1: Plumbing

**Built:** a frame format, a TCP broker that answers PING with PONG, a script to run 3 brokers, and a ping client.

**Framing: the phone game.** TCP is a stream with no gaps, like a friend reading "HELLOWORLDBYE" with no pauses. So we say the length first: `0005HELLO0005WORLD`.
```
[4-byte length][2-byte type][payload]
PING "hi" = [0][0][0][4] [0][1] [104][105]
```
- length = 2 (type) + payload length, meaning everything *after* the length field
- numbers are **big-endian** (biggest part first, like writing "0004")
- `io.ReadFull`, not `Read`: `Read` can return fewer bytes than asked

**Broker:** the accept loop gives each client its own **goroutine** (`go b.handleConn(conn)`). Without `go`, one silent client blocks everyone (you saw this in `TestTwoClients`).

**Go learned:** packages, capital = public, `:=` vs `=`, multiple returns, `if err != nil`, slices, `append`, structs + methods (≈ class, `b` ≈ `this`), `defer`, `switch`, flags, no negative indexes, explicit type conversion (`uint32(x)`).

**Bugs you found by breaking things:**
- length without +2 → the reader gets the wrong bytes, and every later message becomes garbage
- `Read` instead of `ReadFull` → silent wrong data (and the normal test still passed!)
- no `go` → server stuck on one client
- `go run` PID ≠ broker PID → the script builds `bin/broker`
- "process started" ≠ "ready" → the script waits for ports with `nc -z`
- old brokers still running → new ones fail with "address already in use" → the script now refuses to start

**Quiz answers:**
- *Type field vs port per operation:* one port and one reusable connection, easy to add types. Concurrency comes from goroutines, not ports.
- *Data dir per broker:* each broker owns its state. Sharing a dir = corrupted files and no independent failure.
- *Custom binary vs HTTP:* a 6-byte header vs hundreds of bytes of text headers, plus full control. The cost: no ready-made tools.

---

## Phase 2: The log

**Built:** `internal/commitlog`, an append-only file of records.

**A log = a notebook you only write at the end.** Offset = line number (0, 1, 2…).
```
file:   [0005]apple [0006]banana [0006]cherry
index:  offsets = [0, 9, 19]    ← byte position where each record starts
```
- `Append`: lock → write `[len][data]` → remember position → return the offset
- `Read(offset)`: look up the position → `ReadAt` the length → `ReadAt` the data
- `rebuild` (on Open): scan the file to refill the index after a restart
- **Torn write** (crash mid-write, file ends `[0006]che`): check before trusting a record. If it doesn't fit in the file, `Truncate` it off.

**Key ideas:**
- **mutex** = only one goroutine at a time (`l.mu.Lock(); defer l.mu.Unlock()`)
- **data race** = two goroutines touching the same data, one writing. Find it with `go test -race`
- **offset vs byte position:** clients get record numbers, and the positions stay internal
- **Write() ≠ on disk:** data first sits in the OS cache. `fsync` (`file.Sync()`) is safe but slow. Kafka relies on **replication** instead.
- file writes are **not atomic**, so nothing gets rolled back automatically

**Quiz answers:**
- *Append-only:* sequential writes (fast) + old data never touched (a crash only hurts the tail)
- *Offsets:* simple numbers that hide the file layout, so consumers can save "read up to 19"
- *Slow startup on a 100 GB log:* an `.index` file (speeds things up but doesn't replace the scan) + **segments** (scan only the last one)

---

## Phase 3: The producer

**Built:** new frame types, the broker writes to its log, and `cmd/producer`.
```
TypeProduce = 3      payload = message
TypeProduceResp = 4  payload = 8-byte offset (the "ack")
TypeError = 5        payload = error text
```
```
producer ──PRODUCE "apple"──▶ broker ──Append──▶ disk
producer ◀──PRODUCE_RESP 0── broker
```
- `Broker{ID, store *commitlog.Log}`. The broker opens its log in `main.go`.
- **Each broker has its own log**, so offsets count separately (hello = 0 on broker 2)
- after a restart, `rebuild` makes the next offset continue (date = 3)

**Breaking it:**
- **No lock + 20 producers** → a *lost update*: two messages got the same offset, and only 496 of 1000 were kept. `-race` caught it.
- **Kill the broker mid-send** → the producer gets "connection reset" and **can't know** whether the message was stored.

**Delivery guarantees:**
- don't resend → maybe lost = **at-most-once**
- resend → maybe duplicate = **at-least-once**
- **exactly-once** = idempotent producer: (producer ID, sequence number). The broker remembers the last seq per producer. A duplicate gets the *original* offset back.

---

## Next: Phase 4, replication

Right now data lives on **one** broker. If its disk dies, the data is gone.

Plan: the 3 brokers become copies of each other.
```
producer → broker 1 (LEADER) → copies to broker 2, broker 3 (FOLLOWERS)
         ← ack only when the copies are safe
```
Big questions you'll answer:
- `acks=1` (fast, can lose data) vs waiting for followers (safe, slower)
- **majority quorum** (Raft: wait for n/2+1, any two majorities overlap) vs **ISR** (Kafka: wait for all in-sync replicas, kick out slow ones)
- how a follower that was down **catches up**

🎤 Interview one-liners so far:
- "The accept loop must never block on one client, so it's one goroutine per connection."
- "Append-only = sequential I/O + immutable history."
- "The log is the source of truth; the index is a rebuildable cache."
- "Write() only reaches the OS cache; durability needs fsync or replication."
- "Retries give at-least-once; producer ID + sequence numbers make it exactly-once."
