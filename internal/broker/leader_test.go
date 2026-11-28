package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

// startBroker starts a broker on a free port and returns its address and log.
func startBroker(t *testing.T, id string) (*Broker, string, *commitlog.Log) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	store := newTestLog(t)
	b := New(id, store)
	go b.Serve(ln)
	return b, ln.Addr().String(), store
}

func produceTo(t *testing.T, addr, msg string) (uint16, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, msg...)); err != nil { // partition 0
		t.Fatal(err)
	}
	typ, p, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	return typ, p
}

// Leader 1 copies every message to followers 2 and 3: all three logs must be identical.
func TestLeaderReplicates(t *testing.T) {
	_, addr2, log2 := startBroker(t, "2")
	_, addr3, log3 := startBroker(t, "3")
	leader, addr1, log1 := startBroker(t, "1")
	leader.AddFollowers(addr2, addr3)

	msgs := []string{"apple", "banana", "cherry"}
	for i, m := range msgs {
		typ, p := produceTo(t, addr1, m)
		if typ != protocol.TypeProduceResp || binary.BigEndian.Uint64(p) != uint64(i) {
			t.Fatalf("%q: got type %d %q, want ProduceResp offset %d", m, typ, p, i)
		}
	}
	for name, l := range map[string]*commitlog.Log{"leader": log1, "follower 2": log2, "follower 3": log3} {
		if l.Len() != 3 {
			t.Fatalf("%s has %d records, want 3", name, l.Len())
		}
		for i, m := range msgs {
			if got, _ := l.Read(uint64(i)); string(got) != m {
				t.Fatalf("%s offset %d = %q, want %q", name, i, got, m)
			}
		}
	}
}

// If a follower is down, the leader must NOT say "saved" (acks=all).
func TestLeaderFollowerDown(t *testing.T) {
	_, addr2, _ := startBroker(t, "2")
	leader, addr1, _ := startBroker(t, "1")
	leader.AddFollowers(addr2, "127.0.0.1:1") // port 1: nobody listens there = a dead follower

	typ, p := produceTo(t, addr1, "apple")
	if typ != protocol.TypeError {
		t.Fatalf("got type %d %q, want TypeError because a follower is down", typ, p)
	}
}
