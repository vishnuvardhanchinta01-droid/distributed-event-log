// flood = many producers at once, to see what happens when the broker can't keep up.
//
//	go run ./cmd/flood --addr localhost:9101 --producers 200 --msgs 10 --timeout 500ms
//
// Each producer has its own connection and sends its messages one by one:
//   - SAVED   -> next message
//   - BUSY    -> wait (exponential backoff + jitter), try the same message again
//   - TIMEOUT -> the broker didn't answer in time: hang up, reconnect, send it AGAIN
//     (the broker may still save the first copy later = a duplicate)
//
// At the end it reads partition 0 back and counts how many records are really stored.
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
	"sync"
	"sync/atomic"
	"time"

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

func send(conn net.Conn, msg string, timeout time.Duration) error {
	conn.SetDeadline(time.Now().Add(timeout))
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, msg...)); err != nil {
		return err
	}
	typ, p, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	switch typ {
	case protocol.TypeProduceResp:
		return nil
	case protocol.TypeBusy:
		return errBusy
	default:
		return fmt.Errorf("type %d: %s", typ, p)
	}
}

// countStored reads partition 0 from the start and counts the records (FETCH, 500 at a time).
func countStored(addr string) (int, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer conn.Close()
	var from uint64
	total := 0
	for {
		req := make([]byte, 13)
		binary.BigEndian.PutUint64(req[1:9], from)
		binary.BigEndian.PutUint32(req[9:13], 500)
		if err := protocol.WriteFrame(conn, protocol.TypeFetch, req); err != nil {
			return 0, err
		}
		typ, p, err := protocol.ReadFrame(conn)
		if err != nil {
			return 0, err
		}
		if typ != protocol.TypeFetchResp {
			return 0, fmt.Errorf("fetch: type %d: %s", typ, p)
		}
		recs, err := protocol.DecodeRecords(p)
		if err != nil {
			return 0, err
		}
		if len(recs) == 0 {
			return total, nil
		}
		total += len(recs)
		from = recs[len(recs)-1].Offset + 1
	}
}

func main() {
	addr := flag.String("addr", "localhost:9101", "broker to flood (partition 0)")
	producers := flag.Int("producers", 200, "how many producers at the same time")
	msgs := flag.Int("msgs", 10, "messages per producer")
	timeout := flag.Duration("timeout", 300*time.Millisecond, "give up waiting for an answer after this long, then resend")
	maxTime := flag.Duration("max-time", 15*time.Second, "stop the whole test after this long, even if not everything is saved")
	flag.Parse()

	before, err := countStored(*addr)
	if err != nil {
		log.Fatalf("broker not reachable: %v", err)
	}

	var saved, busy, timeouts, lost atomic.Int64 // lost = producers that could not even connect
	var mu sync.Mutex
	var latencies []time.Duration // per message: first try -> saved

	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < *producers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, err := net.Dial("tcp", *addr)
			if err != nil {
				lost.Add(1)
				return
			}
			defer func() { conn.Close() }()
			for m := 0; m < *msgs; m++ {
				msg := fmt.Sprintf("p%d-m%d", i, m)
				t0 := time.Now()
				for attempt := 1; ; attempt++ {
					if time.Since(start) > *maxTime {
						return // out of time: this producer gives up
					}
					err := send(conn, msg, *timeout)
					if err == nil {
						saved.Add(1)
						mu.Lock()
						latencies = append(latencies, time.Since(t0))
						mu.Unlock()
						break
					}
					if errors.Is(err, errBusy) {
						busy.Add(1)
						time.Sleep(backoff(attempt))
						continue
					}
					var ne net.Error
					if errors.As(err, &ne) && ne.Timeout() {
						timeouts.Add(1)
					}
					// timeout or broken connection: hang up, reconnect, resend
					conn.Close()
					c, err := net.Dial("tcp", *addr)
					if err != nil {
						lost.Add(1) // broker gone (or out of connections): this producer stops
						return
					}
					conn = c
				}
			}
		}()
	}
	wg.Wait()
	took := time.Since(start)

	time.Sleep(time.Second) // let the broker finish requests nobody is waiting for anymore
	after, err := countStored(*addr)
	if err != nil {
		after = -1
	}
	slices.Sort(latencies)
	pct := func(q float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(q*float64(len(latencies)-1))].Round(time.Millisecond)
	}
	want := *producers * *msgs
	fmt.Printf("\n%d producers x %d msgs = %d messages, ran %v\n", *producers, *msgs, want, took.Round(time.Millisecond))
	fmt.Printf("  confirmed    : %d of %d   (%.0f msg/s)\n", saved.Load(), want, float64(saved.Load())/took.Seconds())
	fmt.Printf("  BUSY answers : %d\n", busy.Load())
	fmt.Printf("  timeouts     : %d   (each one = a resend)\n", timeouts.Load())
	fmt.Printf("  latency      : p50 %v   p99 %v   max %v\n", pct(0.50), pct(0.99), pct(1))
	if lost.Load() > 0 {
		fmt.Printf("  producers cut off (could not reconnect): %d\n", lost.Load())
	}
	if after < 0 {
		fmt.Printf("  stored       : ?? the broker is not answering anymore (crashed?)\n")
		return
	}
	stored := after - before
	fmt.Printf("  stored       : %d records in the log, %d confirmed -> %d extra copies (duplicates)\n", stored, saved.Load(), int64(stored)-saved.Load())
}
