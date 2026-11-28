# Distributed Event Log with Dynamic Consumer Rebalancing

A Kafka-like distributed event log written from scratch in Go: a replicated append-only log, leader election and failover through ZooKeeper, an in-sync replica set, key-based partitions, consumer groups that rebalance on join/leave, and backpressure under overload. Built to learn *why* Kafka is designed the way it is, by building each part and breaking it.

## Features

- **Append-only log** per partition, with an index rebuilt on startup and torn-write recovery after a crash.
- **Replication** from leader to followers with `acks=1 | quorum | all`; lagging followers are caught up automatically.
- **In-sync replica set (ISR):** failing followers are dropped and rejoin after catching up. Only ISR members may become leader.
- **Leader election and failover** with ZooKeeper ephemeral nodes; a paused "zombie" leader steps down when it wakes.
- **Key-based partitioning**, so events for one key stay in order.
- **Consumer groups** with deterministic assignment, rebalancing and committed offsets (at-least-once).
- **Backpressure:** a bounded number of in-flight requests, an immediate `BUSY` reply, and retries with exponential backoff and jitter.
- **Custom binary protocol** over TCP: `[uint32 length][uint16 type][payload]`.

## Architecture

```
   PRODUCERS "table7:pizza"                         CONSUMER GROUP "cafe"
        │ partition = fnv(key) % 3                  ann → p0, p2   bob → p1
        ▼                                                 ▲ FETCH
   ┌──────────────────────┐   REPLICATE (parallel)   ┌────┴─────────────────┐
   │  BROKER 1 (LEADER)   │ ───────────────────────► │ BROKER 2 (follower)  │
   │   p0 │ p1 │ p2       │ ───────────────────────► │ BROKER 3 (follower)  │
   └──────────┬───────────┘                          └──────────────────────┘
              │
   ┌──────────┴──────────────────────────────────────────────────────────┐
   │ ZOOKEEPER   brokers alive · /leader · ISR · group members · offsets │
   └─────────────────────────────────────────────────────────────────────┘
```

**Write path:** the leader appends the record, sends it to all followers in parallel, and answers the producer once enough in-sync followers confirm (none for `acks=1`, a majority for `quorum`, the whole ISR for `all`).

**Failover:** every broker races to create the ephemeral `/leader` node in ZooKeeper; when the leader's session ends, the node disappears and in-sync brokers race again.

**Consumer groups:** each member watches the member list, sorts the names and deals partitions round-robin, so all members agree without talking. Offsets are committed after each processed batch.

## Results

MacBook, 3 brokers + ZooKeeper on localhost, 3 partitions, 100-byte messages. Full output in [bench/results.md](bench/results.md).

| Measurement | Result |
|---|---|
| Writes (`acks=all`, 1–16 producers) | ~9,000 msg/s, flat; p50 119 µs → 1.6 ms |
| Reads (FETCH 500 records) | ~540k–640k records/s |
| Failover: clean stop / `kill -9` | 100 ms / 3.84 s (ZooKeeper session timeout) |
| Flood, no limit (100 producers) | 53/1000 confirmed, 2,847 duplicates |
| Flood, `--max-inflight 8` | 1000/1000 confirmed, 0 duplicates |

Writes stay flat because one lock covers all partitions; Little's law (16 ÷ 9,000/s ≈ 1.8 ms) matches the measured latency. Reads are ~60x faster because of batching.

## Differences from Kafka

| Here | Kafka |
|---|---|
| Leader pushes each record to followers | Followers pull in batches |
| One leader for all partitions | A leader per partition, spread over brokers |
| One lock across partitions, no write batching | Per-partition logs, batched requests |
| Zombie leader steps down on session expiry | Leader epochs reject stale leaders |
| No high watermark or `min.insync.replicas` | Both, to protect acknowledged writes |
| Consumers compute assignment via ZooKeeper | Group coordinator with generation ids |
| ZooKeeper | KRaft (Raft inside Kafka) |

## Running it

Requires Go and ZooKeeper (`brew install zookeeper`).

```bash
zkServer start
./scripts/cluster.sh start                        # 3 brokers on :9001-9003
./scripts/cluster.sh board                        # leader, ISR, groups, offsets

go run ./cmd/producer table7:pizza table7:cola    # same key → same partition
go run ./cmd/consumer --group cafe --id ann       # run several to see rebalancing

./scripts/cluster.sh stop
go test ./...
```

Failure demos and benchmarks: see [DEMOS.md](DEMOS.md).

## Layout

| Path | What |
|---|---|
| `internal/protocol` | wire frames and message types |
| `internal/commitlog` | append-only log on disk |
| `internal/broker` | produce, replicate, fetch, ISR, backpressure |
| `internal/cluster` | ZooKeeper: election, ISR, groups, offsets |
| `internal/group` | partition assignment |
| `cmd/` | broker, producer, consumer, board, bench, flood |
| `scripts/` | cluster control, demos, benchmarks |
