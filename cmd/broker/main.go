package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"eventlog/internal/broker"
	"eventlog/internal/cluster"
	"eventlog/internal/commitlog"
)

func main() {
	// flags = command-line options, like Python's argparse
	id := flag.String("id", "1", "broker ID")
	port := flag.String("port", "9001", "TCP port to listen on")
	dataDir := flag.String("data", "data/broker-1", "data directory")
	followers := flag.String("followers", "", "comma-separated follower addresses (leader only), e.g. localhost:9002,localhost:9003")
	acks := flag.String("acks", "all", "how many followers must confirm: 1, quorum or all")
	zkAddr := flag.String("zk", "", "ZooKeeper address, e.g. localhost:2181 (empty = no ZooKeeper)")
	partitions := flag.Int("partitions", 3, "how many partitions (notebooks): data/broker-1/p0, p1, ...")
	maxInFlight := flag.Int("max-inflight", 0, "at most N produce requests at once, extra ones get BUSY (0 = no limit)")
	slowDisk := flag.Duration("slow-disk", 0, "every write waits this long first, e.g. 5ms")
	flag.Parse()

	// flag.String gives a *pointer*, so *id reads the value (same as C++)
	// one log per partition, in <data>/p0, <data>/p1, ...
	var parts []*commitlog.Log
	for p := 0; p < *partitions; p++ {
		l, err := commitlog.Open(fmt.Sprintf("%s/p%d", *dataDir, p)) // creates the folder too
		if err != nil {
			log.Fatalf("open log: %v", err)
		}
		defer l.Close()
		parts = append(parts, l)
	}
	ln, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	log.Printf("broker %s listening on :%s (data=%s, %d partitions)", *id, *port, *dataDir, *partitions)
	b := broker.New(*id, parts...)
	if err := b.SetAcks(*acks); err != nil {
		log.Fatal(err)
	}
	b.SetMaxInFlight(*maxInFlight)
	b.SetSlowDisk(*slowDisk)
	if *followers != "" {
		b.AddFollowers(strings.Split(*followers, ",")...)
		log.Printf("broker %s is LEADER, followers: %s, acks=%s", *id, *followers, *acks)
	}

	// pin our note on the ZooKeeper board
	var member *cluster.Member
	if *zkAddr != "" {
		member, err = cluster.Join(*zkAddr, *id, "localhost:"+*port)
		if err != nil {
			log.Fatalf("zookeeper: %v", err)
		}
		log.Printf("broker %s registered in ZooKeeper at %s", *id, *zkAddr)

		b.SetLeader(false) // start as a follower: refuse PRODUCE until we win the chair

		// race for the leader chair (in the background: a follower may wait forever)
		becomeLeader := func() {
			others := func(live map[string]string) map[string]string {
				delete(live, *id) // everyone on the board except me
				return live
			}
			// who was in sync under the OLD leader? they stay in the ISR; anyone else must catch up first
			startInISR := map[string]bool{}
			isr, err := member.ISR()
			if err != nil {
				log.Printf("broker %s: read ISR: %v", *id, err)
			}
			for _, bid := range isr {
				startInISR[bid] = true
			}
			b.OnISRChange(func(ids []string) {
				if err := member.SetISR(ids); err != nil {
					log.Printf("broker %s: save ISR: %v", *id, err)
				}
				log.Printf("broker %s: ISR now %v", *id, ids)
			})
			// set the followers BEFORE accepting writes, so no write is "saved" with zero copies
			if live, err := member.Brokers(); err == nil {
				b.SetFollowers(others(live), startInISR)
			}
			b.SetLeader(true)
			log.Printf("broker %s is now LEADER (acks=%s)", *id, *acks)
			// keep the follower list fresh as brokers come and go (newcomers start OUT of the ISR)
			member.WatchBrokers(func(live map[string]string) {
				b.SetFollowers(others(live), nil)
				log.Printf("broker %s: followers now %v", *id, others(live))
			})
		}

		// what if ZooKeeper gives up on US? (session.go)
		member.OnSessionExpired(func() {
			// Our /leader note is gone: another broker may be leader already.
			if b.IsLeader() {
				b.SetLeader(false)
				log.Printf("broker %s lost its ZooKeeper session: stepping down", *id)
			}
		})
		member.OnNewSession(func() { // come back as a normal broker and race again
			if err := member.Rejoin(); err != nil {
				log.Printf("broker %s: rejoin: %v", *id, err)
			}
			go member.Elect(becomeLeader)
		})

		go member.Elect(becomeLeader)
	}

	// On a normal kill (SIGTERM) or Ctrl-C: say goodbye to ZooKeeper so our note vanishes at once.
	// (kill -9 can't be caught: then ZooKeeper only notices after the 4s session timeout.)
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGTERM, os.Interrupt)
	go func() {
		<-stop
		log.Printf("broker %s shutting down", *id)
		if member != nil {
			member.Close()
		}
		os.Exit(0)
	}()

	log.Fatal(b.Serve(ln))
}
