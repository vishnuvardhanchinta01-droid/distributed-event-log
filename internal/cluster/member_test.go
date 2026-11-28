package cluster

import (
	"fmt"
	"net"
	"os"
	"testing"
	"time"
)

const zkAddr = "localhost:2181"

// These tests need a real ZooKeeper: run `zkServer start` first.
func needZK(t *testing.T) {
	t.Helper()
	c, err := net.DialTimeout("tcp", zkAddr, time.Second)
	if err != nil {
		t.Skip("ZooKeeper not running on " + zkAddr + " (zkServer start)")
	}
	c.Close()
}

func TestRegister(t *testing.T) {
	needZK(t)
	id := fmt.Sprintf("test-%d", os.Getpid()) // a name the real cluster never uses

	a, err := Join(zkAddr, id, "localhost:7777")
	if err != nil {
		t.Fatal(err)
	}
	watcher, err := Join(zkAddr, id+"-watcher", "localhost:7778")
	if err != nil {
		t.Fatal(err)
	}
	defer watcher.Close()

	// 1. the note is on the board, with our address
	live, err := watcher.Brokers()
	if err != nil {
		t.Fatal(err)
	}
	if live[id] != "localhost:7777" {
		t.Fatalf("board has %v, want %s = localhost:7777", live, id)
	}

	// 2. a second broker with the SAME id must be refused
	if b, err := Join(zkAddr, id, "localhost:9999"); err == nil {
		b.Close()
		t.Fatal("second broker with the same id was allowed to register")
	}

	// 3. clean close -> the ephemeral note disappears
	a.Close()
	time.Sleep(200 * time.Millisecond)
	live, _ = watcher.Brokers()
	if _, ok := live[id]; ok {
		t.Fatalf("after Close the board still has %s: %v", id, live)
	}
}
