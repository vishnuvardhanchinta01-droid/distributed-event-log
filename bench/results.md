# Benchmarks

Run: 2026-11-28 00:46, Darwin arm64, Apple M2
3 brokers + ZooKeeper on one machine (localhost), 3 partitions, 100-byte messages, 4000 messages per run, no fsync.

## Writes and reads

| setup | producers | msgs | time | msg/s | MB/s | p50 | p99 | max | BUSY |
|---|---|---|---|---|---|---|---|---|---|
| acks=1 | 1 | 4000 | 0.45s | 8931 | 0.89 | 107µs | 205µs | 1.8ms | 0 |
| acks=1 | 4 | 4000 | 0.45s | 8870 | 0.89 | 448µs | 613µs | 964µs | 0 |
| acks=1 | 16 | 4000 | 0.46s | 8756 | 0.88 | 1.8ms | 2.3ms | 3.6ms | 0 |
| acks=1 | read (1 reader, FETCH 500) | 12000 | 0.02s | 536191 | 53.62 | | | | |
| acks=quorum | 1 | 4000 | 0.50s | 7971 | 0.80 | 118µs | 250µs | 2.4ms | 0 |
| acks=quorum | 4 | 4000 | 0.39s | 10298 | 1.03 | 384µs | 548µs | 751µs | 0 |
| acks=quorum | 16 | 4000 | 0.39s | 10220 | 1.02 | 1.5ms | 2.1ms | 3.1ms | 0 |
| acks=quorum | read (1 reader, FETCH 500) | 12000 | 0.02s | 616015 | 61.60 | | | | |
| acks=all | 1 | 4000 | 0.51s | 7814 | 0.78 | 119µs | 253µs | 2.8ms | 0 |
| acks=all | 4 | 4000 | 0.44s | 9115 | 0.91 | 421µs | 759µs | 2.1ms | 0 |
| acks=all | 16 | 4000 | 0.43s | 9368 | 0.94 | 1.6ms | 3.2ms | 4.3ms | 0 |
| acks=all | read (1 reader, FETCH 500) | 12000 | 0.02s | 635909 | 63.59 | | | | |

## Failover: longest time without a successful write

- **stop**: LONGEST TIME WITHOUT A SUCCESSFUL WRITE: 100ms  (from 5.4s to 5.5s into the test)
- **kill9**: LONGEST TIME WITHOUT A SUCCESSFUL WRITE: 3.84s  (from 5.0s to 8.9s into the test)
