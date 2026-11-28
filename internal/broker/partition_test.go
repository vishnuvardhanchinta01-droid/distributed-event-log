package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

// startBrokerN starts a broker with n partitions.
func startBrokerN(t *testing.T, id string, n int) (*Broker, string, []*commitlog.Log) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	var parts []*commitlog.Log
	for i := 0; i < n; i++ {
		parts = append(parts, newTestLog(t))
	}
	b := New(id, parts...)
	go b.Serve(ln)
	return b, ln.Addr().String(), parts
}

func produceToPart(t *testing.T, addr string, p byte, msg string) (uint16, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(5 * time.Second))
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{p}, msg...)); err != nil {
		t.Fatal(err)
	}
	typ, resp, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	return typ, resp
}

// Each partition is its own notebook with its own offsets, and is replicated separately.
func TestPartitions(t *testing.T) {
	_, addr2, f := startBrokerN(t, "2", 3)
	leader, addr1, l := startBrokerN(t, "1", 3)
	leader.AddFollowers(addr2)

	sends := []struct {
		p    byte
		msg  string
		want uint64
	}{{0, "a0", 0}, {1, "b0", 0}, {1, "b1", 1}, {2, "c0", 0}, {0, "a1", 1}}
	for _, s := range sends {
		typ, resp := produceToPart(t, addr1, s.p, s.msg)
		if typ != protocol.TypeProduceResp || binary.BigEndian.Uint64(resp) != s.want {
			t.Fatalf("%s to partition %d: got type %d %q, want offset %d", s.msg, s.p, typ, resp, s.want)
		}
	}
	for i, want := range []uint64{2, 2, 1} {
		if l[i].Len() != want || f[i].Len() != want {
			t.Fatalf("partition %d: leader has %d, follower has %d, want %d", i, l[i].Len(), f[i].Len(), want)
		}
	}
	if got, _ := f[1].Read(1); string(got) != "b1" {
		t.Fatalf("follower partition 1 offset 1 = %q, want b1", got)
	}
	if typ, _ := produceToPart(t, addr1, 7, "nope"); typ != protocol.TypeError {
		t.Fatalf("partition 7: got type %d, want an error (only 0..2 exist)", typ)
	}
}
