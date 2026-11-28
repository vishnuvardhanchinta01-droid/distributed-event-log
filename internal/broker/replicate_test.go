package broker

import (
	"encoding/binary"
	"net"
	"strings"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

// replicate sends one REPLICATE frame (as a leader would) and returns the reply.
func replicate(t *testing.T, conn net.Conn, off uint64, msg string) (uint16, []byte) {
	t.Helper()
	payload := make([]byte, 1+8+len(msg)) // [partition 0][offset][msg]
	binary.BigEndian.PutUint64(payload[1:9], off)
	copy(payload[9:], msg)
	if err := protocol.WriteFrame(conn, protocol.TypeReplicate, payload); err != nil {
		t.Fatal(err)
	}
	typ, p, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	return typ, p
}

// We pretend to be the leader and send REPLICATE frames to a follower.
func TestFollowerReplicate(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	store := newTestLog(t)
	go New("2", store).Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))

	// 1. in-order records are stored and acked with the same offset
	for i, msg := range []string{"apple", "banana"} {
		typ, p := replicate(t, conn, uint64(i), msg)
		if typ != protocol.TypeReplicateAck || binary.BigEndian.Uint64(p) != uint64(i) {
			t.Fatalf("%q: got type %d payload %v, want ReplicateAck %d", msg, typ, p, i)
		}
	}

	// 2. a gap (offset 5 when the follower has 2 records) must be refused with BEHIND "I expect 2"
	typ, p := replicate(t, conn, 5, "oops")
	if typ != protocol.TypeReplicateBehind || len(p) != 8 || binary.BigEndian.Uint64(p) != 2 {
		t.Fatalf("gap: got type %d %v, want ReplicateBehind with offset 2", typ, p)
	}

	// 2b. a record the follower already has (offset 0 again) is still an 'out of order' error
	typ, p = replicate(t, conn, 0, "apple")
	if typ != protocol.TypeError || !strings.Contains(string(p), "out of order") {
		t.Fatalf("duplicate: got type %d %q, want an 'out of order' error", typ, p)
	}

	// 3. the connection still works, and the follower log is exactly [apple banana]
	typ, _ = replicate(t, conn, 2, "cherry")
	if typ != protocol.TypeReplicateAck {
		t.Fatalf("cherry: got type %d, want ReplicateAck", typ)
	}
	if store.Len() != 3 {
		t.Fatalf("follower has %d records, want 3", store.Len())
	}
	if got, _ := store.Read(1); string(got) != "banana" {
		t.Fatalf("Read(1) = %q, want banana", got)
	}
}
