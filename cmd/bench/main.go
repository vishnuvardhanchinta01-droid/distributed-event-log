// bench measures how fast the cluster writes and reads.
//
//	go run ./cmd/bench --label acks=all --producers 1,4,16 --msgs 4000 --size 100
//	go run ./cmd/bench --failover 15s     (then kill the leader: how long can nobody write?)
//
// WRITE: for each producer count, P producers (one connection each) share --msgs messages,
// spread over the partitions. Every message is timed from send to "saved" (ProduceResp).
// READ:  one reader FETCHes every partition from offset 0, 500 records at a time.
//
// It prints Markdown table rows, so scripts/bench.sh can paste them into bench/results.md.
package main

import (
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"eventlog/internal/cluster"
	"eventlog/internal/protocol"
)

var errBusy = errors.New("busy")

func backoff(attempt int) time.Duration { // same as cmd/producer/backoff.go
	wait := 50 * time.Millisecond << (attempt - 1)
	if wait > 2*time.Second || wait <= 0 {
		wait = 2 * time.Second
	}
	return wait/2 + rand.N(wait/2+1)
}

func produce(conn net.Conn, p int, msg []byte) error {
	conn.SetDeadline(time.Now().Add(10 * time.Second))
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{byte(p)}, msg...)); err != nil {
		return err
	}
	typ, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	switch typ {
	case protocol.TypeProduceResp:
		return nil
	case protocol.TypeBusy:
		return errBusy
	default:
		return fmt.Errorf("type %d: %s", typ, payload)
	}
}

// writeRun: P producers share n messages of size bytes. Returns how long it took + every latency.
func writeRun(addr string, P, n, size, partitions int) (time.Duration, []time.Duration, int, error) {
	var mu sync.Mutex
	var lat []time.Duration
	busy := 0
	var firstErr error
	msg := []byte(strings.Repeat("x", size))

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < P; i++ {
		share := n / P
		if i < n%P {
			share++
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", addr)
			if err != nil {
				mu.Lock()
				firstErr = err
				mu.Unlock()
				return
			}
			defer conn.Close()
			mine := make([]time.Duration, 0, share)
			myBusy := 0
			for m := 0; m < share; m++ {
				p := (i + m*P) % partitions // spread over all partitions
				t0 := time.Now()
				for attempt := 1; ; attempt++ {
					err := produce(conn, p, msg)
					if err == nil {
						break
					}
					if errors.Is(err, errBusy) {
						myBusy++
						time.Sleep(backoff(attempt))
						continue
					}
					mu.Lock()
					firstErr = err
					mu.Unlock()
					return
				}
				mine = append(mine, time.Since(t0))
			}
			mu.Lock()
			lat = append(lat, mine...)
			busy += myBusy
			mu.Unlock()
		}()
	}
	wg.Wait()
	return time.Since(start), lat, busy, firstErr
}

// readAll FETCHes every partition from 0 to the end. Returns records read and the time it took.
func readAll(addr string, partitions int) (int, int, time.Duration, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, 0, 0, err
	}
	defer conn.Close()
	total, bytes := 0, 0
	start := time.Now()
	for p := 0; p < partitions; p++ {
		var from uint64
		for {
			req := make([]byte, 13)
			req[0] = byte(p)
			binary.BigEndian.PutUint64(req[1:9], from)
			binary.BigEndian.PutUint32(req[9:13], 500)
			if err := protocol.WriteFrame(conn, protocol.TypeFetch, req); err != nil {
				return 0, 0, 0, err
			}
			typ, payload, err := protocol.ReadFrame(conn)
			if err != nil {
				return 0, 0, 0, err
			}
			if typ != protocol.TypeFetchResp {
				return 0, 0, 0, fmt.Errorf("fetch: type %d: %s", typ, payload)
			}
			recs, err := protocol.DecodeRecords(payload)
			if err != nil {
				return 0, 0, 0, err
			}
			if len(recs) == 0 {
				break
			}
			for _, r := range recs {
				bytes += len(r.Data)
			}
			total += len(recs)
			from = recs[len(recs)-1].Offset + 1
		}
	}
	return total, bytes, time.Since(start), nil
}

func pct(sorted []time.Duration, q float64) string {
	if len(sorted) == 0 {
		return "-"
	}
	d := sorted[int(q*float64(len(sorted)-1))]
	if d < time.Millisecond {
		return fmt.Sprintf("%dµs", d.Microseconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func main() {
	addr := flag.String("addr", "", "broker to use (empty = ask ZooKeeper who the leader is)")
	zkAddr := flag.String("zk", "localhost:2181", "ZooKeeper address")
	label := flag.String("label", "", "first column of every row, e.g. acks=all")
	producersFlag := flag.String("producers", "1,4,16", "producer counts to try, comma-separated")
	n := flag.Int("msgs", 4000, "messages per run")
	size := flag.Int("size", 100, "bytes per message")
	partitions := flag.Int("partitions", 3, "partitions to spread over")
	header := flag.Bool("header", false, "print the table header first")
	failover := flag.Duration("failover", 0, "failover mode: write a message every 20ms for this long and report the longest gap (kill the leader meanwhile)")
	flag.Parse()

	if *failover > 0 {
		failoverRun(*zkAddr, *failover, 20*time.Millisecond)
		return
	}

	if *addr == "" {
		board, err := cluster.Connect(*zkAddr)
		if err != nil {
			log.Fatalf("zookeeper: %v", err)
		}
		for i := 0; i < 50 && *addr == ""; i++ {
			*addr, _ = board.LeaderAddr()
			if *addr == "" {
				time.Sleep(100 * time.Millisecond)
			}
		}
		board.Close()
		if *addr == "" {
			log.Fatal("no leader")
		}
	}

	if *header {
		fmt.Println("| setup | producers | msgs | time | msg/s | MB/s | p50 | p99 | max | BUSY |")
		fmt.Println("|---|---|---|---|---|---|---|---|---|---|")
	}
	for _, f := range strings.Split(*producersFlag, ",") {
		P, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || P < 1 {
			log.Fatalf("bad --producers value %q", f)
		}
		took, lat, busy, err := writeRun(*addr, P, *n, *size, *partitions)
		if err != nil {
			log.Fatalf("write run with %d producers: %v", P, err)
		}
		slices.Sort(lat)
		rate := float64(len(lat)) / took.Seconds()
		fmt.Printf("| %s | %d | %d | %.2fs | %.0f | %.2f | %s | %s | %s | %d |\n",
			*label, P, len(lat), took.Seconds(), rate, rate*float64(*size)/1e6,
			pct(lat, 0.50), pct(lat, 0.99), pct(lat, 1), busy)
	}

	recs, bytes, took, err := readAll(*addr, *partitions)
	if err != nil {
		log.Fatalf("read: %v", err)
	}
	fmt.Printf("| %s | read (1 reader, FETCH 500) | %d | %.2fs | %.0f | %.2f | | | | |\n",
		*label, recs, took.Seconds(), float64(recs)/took.Seconds(), float64(bytes)/1e6/took.Seconds())
}
