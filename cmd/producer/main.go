package main

import (
	"encoding/binary"
	"errors"
	"eventlog/internal/cluster"
	"eventlog/internal/protocol"
	"flag"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"strings"
	"time"
)

// partitionFor picks the notebook for a key: the SAME key must always give the SAME partition,
// so all messages for one key stay in order.
func partitionFor(key string, n int) int {
	h := fnv.New32()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(n))
}

// produce sends one message to partition p of the broker and returns the offset it was stored at.
func produce(conn net.Conn, p int, msg string) (uint64, error) {
	// payload = [1-byte partition][message]
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{byte(p)}, msg...)); err != nil {
		return 0, err
	}
	typ, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return 0, err
	}
	switch typ {
	case protocol.TypeProduceResp:
		return binary.BigEndian.Uint64(payload), nil
	case protocol.TypeError:
		return 0, fmt.Errorf("broker error: %s", payload)
	case protocol.TypeBusy:
		return 0, errBusy
	default:
		return 0, fmt.Errorf("unexpected reply type %d", typ)
	}
}

// sendWithRetry finds the leader, sends msg, and if anything goes wrong
// (leader died, "not leader", timeout) waits a bit, asks ZooKeeper again and retries.
func sendWithRetry(findLeader func() (string, error), p int, msg string) (uint64, error) {
	var lastErr error
	pause := func() { time.Sleep(500 * time.Millisecond) } // give the cluster time to elect a new leader
	for attempt := 1; attempt <= 20; attempt++ {
		addr, err := findLeader()
		if err != nil {
			lastErr = err
			pause()
			continue
		}
		if addr == "" {
			lastErr = fmt.Errorf("no leader right now")
			pause()
			continue
		}
		conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
		if err != nil {
			lastErr = err
			pause()
			continue
		}
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		off, err := produce(conn, p, msg)
		conn.Close()
		if err == nil {
			return off, nil
		}
		if errors.Is(err, errBusy) {
			lastErr = err
			time.Sleep(backoff(attempt))
			continue
		}
		log.Printf("attempt %d via %s failed: %v (retrying)", attempt, addr, err)
		lastErr = err
		pause()
	}
	return 0, fmt.Errorf("gave up after 20 attempts: %w", lastErr)
}

// Usage: go run ./cmd/producer apple banana cherry            (asks ZooKeeper who the leader is)
//
//	go run ./cmd/producer --addr localhost:9001 apple    (talks to one broker directly)
//	go run ./cmd/producer table7:pizza table7:cola     (key:value → same key, same partition)
func main() {
	addr := flag.String("addr", "", "talk to this broker directly (skips ZooKeeper)")
	zkAddr := flag.String("zk", "localhost:2181", "ZooKeeper address, used to find the leader")
	partitions := flag.Int("partitions", 3, "how many partitions the brokers have")
	flag.Parse()
	msgs := flag.Args() // everything after the flags = the messages
	if len(msgs) == 0 {
		log.Fatal("usage: producer [--addr host:port | --zk host:port] msg1 msg2 ...")
	}

	findLeader := func() (string, error) { return *addr, nil } // --addr given: always that broker
	if *addr == "" {
		board, err := cluster.Connect(*zkAddr)
		if err != nil {
			log.Fatalf("zookeeper: %v", err)
		}
		defer board.Close()
		findLeader = board.LeaderAddr // ask the notice board every time
	}

	for _, m := range msgs {
		key := m // "table7:pizza" → key "table7"; no colon → the whole message is the key
		if i := strings.Index(m, ":"); i >= 0 {
			key = m[:i]
		}
		p := partitionFor(key, *partitions)
		off, err := sendWithRetry(findLeader, p, m)
		if err != nil {
			log.Fatalf("produce %q: %v", m, err)
		}
		fmt.Printf("%q -> partition %d, offset %d\n", m, p, off)
	}
}
