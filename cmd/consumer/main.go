// consumer reads one notebook (partition) page by page, using FETCH.
//
//	go run ./cmd/consumer --partition 1            (read notebook 1 from page 0, then stop)
//	go run ./cmd/consumer --partition 1 --from 5   (start at page 5)
//	go run ./cmd/consumer --partition 1 --follow   (keep waiting for new pages, Ctrl-C to stop)
//
// as a member of a consumer GROUP (it gets its partitions from the group):
//
//	go run ./cmd/consumer --group kitchen --id ann
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"eventlog/internal/cluster"
	"eventlog/internal/group"
	"eventlog/internal/protocol"
)

// fetch asks the leader for up to `max` records of partition p, starting at `from`.
func fetch(addr string, p int, from uint64, max uint32) ([]protocol.Record, error) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	req := make([]byte, 13) // [partition][from offset][max]
	req[0] = byte(p)
	binary.BigEndian.PutUint64(req[1:9], from)
	binary.BigEndian.PutUint32(req[9:13], max)
	if err := protocol.WriteFrame(conn, protocol.TypeFetch, req); err != nil {
		return nil, err
	}
	typ, resp, err := protocol.ReadFrame(conn)
	if err != nil {
		return nil, err
	}
	if typ == protocol.TypeError {
		return nil, fmt.Errorf("broker error: %s", resp)
	}
	if typ != protocol.TypeFetchResp {
		return nil, fmt.Errorf("unexpected reply type %d", typ)
	}
	return protocol.DecodeRecords(resp)
}

// readPartition reads partition p from `next` onwards, forever, until ctx is cancelled
// (a rebalance took this partition away from us).
// slow = pretend each page takes this long to process (for the crash demo)
var slow time.Duration

func readPartition(ctx context.Context, board *cluster.Member, findLeader func() (string, error), groupName, who string, p int) {
	next, err := board.CommittedOffset(groupName, p)
	if err != nil {
		log.Printf("[%s] p%d read bookmark: %v", who, p, err)
		return
	}
	log.Printf("[%s] p%d starting at bookmark %d", who, p, next)

	for {
		select {
		case <-ctx.Done():
			return // not my partition any more
		default:
		}
		leader, err := findLeader()
		if err == nil && leader == "" {
			err = fmt.Errorf("no leader right now")
		}
		var recs []protocol.Record
		if err == nil {
			recs, err = fetch(leader, p, next, 100)
		}
		if err != nil {
			log.Printf("[%s] p%d fetch failed: %v (retrying)", who, p, err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, r := range recs {
			fmt.Printf("[%s] p%d offset %d: %s\n", who, p, r.Offset, r.Data) // "processing" the page
			time.Sleep(slow)
			next = r.Offset + 1
		}
		if len(recs) > 0 {
			if err := board.CommitOffset(groupName, p, next); err != nil {
				log.Printf("[%s] p%d commit: %v", who, p, err)
			}
		}
		if len(recs) == 0 {
			time.Sleep(500 * time.Millisecond) // nothing new: poll again soon
		}
	}
}

// runGroup joins the group and, on every rebalance, stops reading the old partitions
// and starts one reader goroutine per partition the group gave us.
func runGroup(board *cluster.Member, findLeader func() (string, error), groupName, me string, partitions int) {
	if err := board.JoinGroup(groupName, me); err != nil {
		log.Fatalf("join group: %v", err)
	}
	var stop context.CancelFunc = func() {}
	var readers sync.WaitGroup
	board.WatchGroup(groupName, func(members []string) {
		mine := group.Assign(members, partitions)[me] // deterministic: same answer on every member
		log.Printf("[%s] REBALANCE: members %v → I read partitions %v", me, members, mine)

		stop()         // 1. tell my old readers to stop...
		readers.Wait() // 2. ...and wait until they really did
		ctx, cancel := context.WithCancel(context.Background())
		stop = cancel
		for _, p := range mine { // 3. start a reader for each of my partitions
			readers.Add(1)
			go func(p int) {
				defer readers.Done()
				readPartition(ctx, board, findLeader, groupName, me, p)
			}(p)
		}
	})
	// keep running until Ctrl-C; then say goodbye to ZooKeeper so the team rebalances at once
	// (a crash instead: the others wait for the 4s session timeout)
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit
	log.Printf("[%s] leaving the group", me)
	board.Close()
}

func main() {
	addr := flag.String("addr", "", "read from this broker directly (skips ZooKeeper)")
	zkAddr := flag.String("zk", "localhost:2181", "ZooKeeper address, used to find the leader")
	part := flag.Int("partition", 0, "which notebook to read")
	from := flag.Uint64("from", 0, "first offset to read")
	follow := flag.Bool("follow", false, "keep waiting for new records")
	groupName := flag.String("group", "", "join this consumer group (then --partition is ignored)")
	me := flag.String("id", "", "my name inside the group, e.g. ann")
	partitions := flag.Int("partitions", 3, "how many partitions the brokers have")
	flag.DurationVar(&slow, "slow", 0, "pretend each record takes this long to process, e.g. 1s")
	flag.Parse()

	board, err := cluster.Connect(*zkAddr)
	if err != nil {
		log.Fatalf("zookeeper: %v", err)
	}
	defer board.Close()
	findLeader := board.LeaderAddr
	if *addr != "" {
		findLeader = func() (string, error) { return *addr, nil }
	}

	if *groupName != "" {
		if *me == "" {
			log.Fatal("--group needs --id, e.g. --group kitchen --id ann")
		}
		runGroup(board, findLeader, *groupName, *me, *partitions)
		return
	}

	next := *from // in-memory bookmark (single mode; groups store it in ZooKeeper)
	for {
		leader, err := findLeader()
		if err == nil && leader == "" {
			err = fmt.Errorf("no leader right now")
		}
		var recs []protocol.Record
		if err == nil {
			recs, err = fetch(leader, *part, next, 100)
		}
		if err != nil {
			log.Printf("fetch failed: %v (retrying)", err)
			time.Sleep(500 * time.Millisecond)
			continue
		}
		for _, r := range recs {
			fmt.Printf("p%d offset %d: %s\n", *part, r.Offset, r.Data)
			next = r.Offset + 1 // move the bookmark past this page
		}
		if len(recs) == 0 { // reached the end of the notebook
			if !*follow {
				return
			}
			time.Sleep(500 * time.Millisecond) // nothing new: wait a bit, ask again (polling)
		}
	}
}
