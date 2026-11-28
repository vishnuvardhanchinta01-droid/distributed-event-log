package main

import (
	"fmt"
	"log"
	"net"
	"time"

	"eventlog/internal/cluster"
)

// failoverRun writes one small message every `every` for `duration`, always to whoever
// ZooKeeper says is leader. When the leader dies, writes fail until a new leader is elected.
// It reports the longest gap between two successful writes = how long the cluster could not accept writes.
func failoverRun(zkAddr string, duration, every time.Duration) {
	board, err := cluster.Connect(zkAddr)
	if err != nil {
		log.Fatalf("zookeeper: %v", err)
	}
	defer board.Close()

	var conn net.Conn
	var connAddr string
	hangUp := func() {
		if conn != nil {
			conn.Close()
			conn = nil
		}
	}
	defer hangUp()

	start := time.Now()
	lastOK := time.Time{}
	var worst time.Duration
	var worstFrom, worstTo time.Duration // seconds into the test
	ok, failed := 0, 0
	leaders := []string{}

	for i := 0; time.Since(start) < duration; i++ {
		time.Sleep(every)
		addr, err := board.LeaderAddr()
		if err != nil || addr == "" {
			failed++
			hangUp()
			continue
		}
		if conn == nil || connAddr != addr {
			hangUp()
			if conn, err = net.DialTimeout("tcp", addr, 300*time.Millisecond); err != nil {
				conn = nil
				failed++
				continue
			}
			connAddr = addr
		}
		conn.SetDeadline(time.Now().Add(500 * time.Millisecond))
		if err := produce(conn, 0, []byte(fmt.Sprintf("tick-%d", i))); err != nil {
			failed++
			hangUp()
			continue
		}
		ok++
		now := time.Now()
		if !lastOK.IsZero() {
			if gap := now.Sub(lastOK); gap > worst {
				worst, worstFrom, worstTo = gap, lastOK.Sub(start), now.Sub(start)
			}
		}
		lastOK = now
		if id, _ := board.LeaderID(); len(leaders) == 0 || leaders[len(leaders)-1] != id {
			leaders = append(leaders, id)
		}
	}
	fmt.Printf("writes ok: %d, failed attempts: %d, leaders seen: %v\n", ok, failed, leaders)
	fmt.Printf("LONGEST TIME WITHOUT A SUCCESSFUL WRITE: %v  (from %.1fs to %.1fs into the test)\n",
		worst.Round(10*time.Millisecond), worstFrom.Seconds(), worstTo.Seconds())
}
