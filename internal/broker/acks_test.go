package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

const deadAddr = "127.0.0.1:1" // nobody listens on port 1 = a dead follower

// slowFollower is a fake follower that waits `delay` before every ACK.
func slowFollower(t *testing.T, delay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				for {
					_, p, err := protocol.ReadFrame(conn)
					if err != nil || len(p) < 9 {
						return
					}
					time.Sleep(delay)
					protocol.WriteFrame(conn, protocol.TypeReplicateAck, p[1:9]) // [partition][OFFSET]...
				}
			}()
		}
	}()
	return ln.Addr().String()
}

func leaderWith(t *testing.T, acks string, followers ...string) string {
	t.Helper()
	leader, addr, _ := startBroker(t, "1")
	if err := leader.SetAcks(acks); err != nil {
		t.Fatal(err)
	}
	leader.AddFollowers(followers...)
	return addr
}

func wantSaved(t *testing.T, addr, msg string) {
	t.Helper()
	typ, p := produceTo(t, addr, msg)
	if typ != protocol.TypeProduceResp {
		t.Fatalf("%q: got type %d %q, want ProduceResp (saved)", msg, typ, p)
	}
	_ = binary.BigEndian
}

func wantError(t *testing.T, addr, msg string) {
	t.Helper()
	if typ, p := produceTo(t, addr, msg); typ != protocol.TypeError {
		t.Fatalf("%q: got type %d %q, want TypeError", msg, typ, p)
	}
}

func TestAcksModes(t *testing.T) {
	t.Run("acks=1, both followers dead -> still saved", func(t *testing.T) {
		wantSaved(t, leaderWith(t, "1", deadAddr, deadAddr), "apple")
	})
	t.Run("acks=quorum, one dead -> saved", func(t *testing.T) {
		_, f2, log2 := startBroker(t, "2")
		wantSaved(t, leaderWith(t, "quorum", f2, deadAddr), "apple")
		if log2.Len() != 1 {
			t.Fatalf("live follower has %d records, want 1", log2.Len())
		}
	})
	t.Run("acks=quorum, both dead -> error", func(t *testing.T) {
		wantError(t, leaderWith(t, "quorum", deadAddr, deadAddr), "apple")
	})
	t.Run("acks=all, one dead -> error", func(t *testing.T) {
		_, f2, _ := startBroker(t, "2")
		wantError(t, leaderWith(t, "all", f2, deadAddr), "apple")
	})
}

// Two followers that each take 300ms. Called in parallel, the producer
// should wait ~300ms (the slowest), not 600ms (the sum).
func TestParallel(t *testing.T) {
	f2 := slowFollower(t, 300*time.Millisecond)
	f3 := slowFollower(t, 300*time.Millisecond)
	addr := leaderWith(t, "all", f2, f3)

	start := time.Now()
	wantSaved(t, addr, "apple")
	if took := time.Since(start); took > 500*time.Millisecond {
		t.Fatalf("acks=all took %v: followers were called one after the other, not in parallel", took)
	}
}

// With acks=1 the producer should not wait for slow followers at all.
func TestAcks1IsFast(t *testing.T) {
	f2 := slowFollower(t, 300*time.Millisecond)
	addr := leaderWith(t, "1", f2)

	start := time.Now()
	wantSaved(t, addr, "apple")
	if took := time.Since(start); took > 150*time.Millisecond {
		t.Fatalf("acks=1 took %v: the producer waited for the follower", took)
	}
}
