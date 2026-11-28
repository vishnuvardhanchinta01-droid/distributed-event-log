package cluster

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// Two brokers race for the chair; when the leader leaves, the other one takes over.
func TestElection(t *testing.T) {
	needZK(t)
	tag := fmt.Sprintf("etest-%d", os.Getpid())
	join := func(name string) *Member {
		m, err := Join(zkAddr, tag+"-"+name, "localhost:0")
		if err != nil {
			t.Fatal(err)
		}
		m.leaderPath = "/" + tag + "-leader" // our own chair, not the real cluster's
		return m
	}
	a, b := join("a"), join("b")
	defer b.Close()

	won := make(chan string, 2)
	go a.Elect(func() { won <- "a" })
	select {
	case w := <-won:
		if w != "a" {
			t.Fatalf("%s won, want a", w)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("a never became leader")
	}

	go b.Elect(func() { won <- "b" })
	select {
	case w := <-won:
		t.Fatalf("%s became leader while a still has the chair: two leaders!", w)
	case <-time.After(500 * time.Millisecond): // good: b is waiting
	}
	if id, _ := b.LeaderID(); id != tag+"-a" {
		t.Fatalf("board says leader = %q, want %q", id, tag+"-a")
	}

	a.Close() // the leader leaves → its note vanishes → b's bell rings
	select {
	case w := <-won:
		if w != "b" {
			t.Fatalf("%s won, want b", w)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("b never took over after a left")
	}
}
