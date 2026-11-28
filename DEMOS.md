# Demos

Every demo starts its own fresh cluster, so run them one at a time. Before each one, make sure no old cluster is running: `./scripts/cluster.sh stop`. You need ZooKeeper running (`zkServer start`), except for the backpressure demo.

| # | Command | What it shows | Watch for | Interview point |
|---|---|---|---|---|
| 1 | `./scripts/demo_failover.sh` | The leader is stopped while a producer is sending | A new leader is elected; every message is still saved, with a short pause | Leader election = an ephemeral node + watch in ZooKeeper |
| 2 | `KILL9=1 ./scripts/demo_failover.sh` | The leader **crashes** (no goodbye) | The pause is longer, about 4–6s: ZooKeeper waits for the session timeout | Detection time = session timeout; the retry budget must be longer than that |
| 3 | `./scripts/demo_isr.sh` | A follower falls behind, then comes back | It leaves the ISR (writes go on), **can not become leader** while behind, then catches up and rejoins | acks=all means "all of the **ISR**", so one dead follower can't stop writes |
| 4 | `./scripts/demo_zombie.sh` | The leader is frozen past its session (a zombie) | It steps down when it wakes up, rejoins and catches up | Fencing: an old leader must not keep writing (epochs fix this fully) |
| 5 | `./scripts/demo_group.sh` | A consumer is killed with -9 in the middle of a batch | A rebalance happens; the other consumer re-reads the uncommitted pages, so you see duplicates | At-least-once: commit after processing → duplicates on a crash, never loss |
| 6 | `./scripts/demo_backpressure.sh` | A flood against a slow broker, with and without a limit | No limit: about 53 of 1000 confirmed, about 2800 duplicates. Limit 8: 1000 of 1000, 0 duplicates | Bounded in-flight + fast BUSY + jittered backoff prevents congestion collapse |


# Benchmarks

| Command | Measures |
|---|---|
| `./scripts/bench.sh` | Write throughput and latency (p50/p99) for acks=1 / quorum / all with 1, 4 and 16 producers, plus read throughput |
| `./scripts/bench_failover.sh` | The longest time **nobody can write** when the leader dies: clean stop vs kill -9 |

Both write to `bench/results.md`. You can change the settings, e.g. `MSGS=10000 PRODUCERS=1,8,32 ./scripts/bench.sh`.
