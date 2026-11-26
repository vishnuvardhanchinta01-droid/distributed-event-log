package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

// newTestLog opens a fresh log in a temp folder (used by all broker tests).
func newTestLog(t *testing.T) *commitlog.Log {
	l, err := commitlog.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { l.Close() })
	return l
}

func TestProduce(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	store := newTestLog(t)
	go New("7", store).Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second)) // don't hang forever if the broker never replies

	for i, msg := range []string{"apple", "banana", "cherry"} {
		if err := protocol.WriteFrame(conn, protocol.TypeProduce, []byte(msg)); err != nil {
			t.Fatal(err)
		}
		typ, p, err := protocol.ReadFrame(conn)
		if err != nil || typ != protocol.TypeProduceResp || len(p) != 8 {
			t.Fatalf("%q: got type %d, payload %v, err %v", msg, typ, p, err)
		}
		if off := binary.BigEndian.Uint64(p); off != uint64(i) {
			t.Fatalf("%q: got offset %d, want %d", msg, off, i)
		}
	}
	// the broker really stored them in its log
	got, err := store.Read(1)
	if err != nil || string(got) != "banana" {
		t.Fatalf("store.Read(1) = %q, %v; want banana", got, err)
	}
}
