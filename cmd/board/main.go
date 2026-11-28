// board prints what's on the ZooKeeper notice board: who is alive, who is leader.
package main

import (
	"flag"
	"fmt"
	"log"
	"sort"

	"eventlog/internal/cluster"
)

func main() {
	zkAddr := flag.String("zk", "localhost:2181", "ZooKeeper address")
	resetISR := flag.Bool("reset-isr", false, "delete the saved ISR (use with fresh, empty logs)")
	flag.Parse()
	m, err := cluster.Connect(*zkAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()
	if *resetISR {
		if err := m.ResetISR(); err != nil {
			log.Fatal(err)
		}
		fmt.Println("ISR reset")
		return
	}

	live, err := m.Brokers()
	if err != nil {
		log.Fatal(err)
	}
	ids := make([]string, 0, len(live))
	for id := range live {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	fmt.Println("alive brokers:")
	for _, id := range ids {
		fmt.Printf("   %s  %s\n", id, live[id])
	}
	leader, err := m.LeaderID()
	if err != nil {
		log.Fatal(err)
	}
	if leader == "" {
		leader = "(nobody)"
	}
	fmt.Println("leader:", leader)
	isr, err := m.ISR()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("in-sync (ISR):", isr)
	groups, err := m.Groups()
	if err != nil {
		log.Fatal(err)
	}
	for _, g := range groups {
		members, _ := m.GroupMembers(g)
		var marks []string
		for p := 0; p < 3; p++ {
			off, _ := m.CommittedOffset(g, p)
			marks = append(marks, fmt.Sprintf("p%d=%d", p, off))
		}
		fmt.Printf("group %s: members %v, bookmarks %v\n", g, members, marks)
	}
}
