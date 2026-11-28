package broker

import (
	"encoding/binary"
	"testing"

	"eventlog/internal/protocol"
)

// The follower missed apple and banana (it wasn't a follower yet).
// The next produce must make the leader resend them, so all logs match.
func TestCatchUp(t *testing.T) {
	_, addr2, log2 := startBroker(t, "2")
	leader, addr1, log1 := startBroker(t, "1")

	// the leader writes 2 records the follower never sees
	log1.Append([]byte("apple"))
	log1.Append([]byte("banana"))
	leader.AddFollowers(addr2) // acks=all: produce only succeeds if follower 2 really has it

	typ, p := produceTo(t, addr1, "cherry")
	if typ != protocol.TypeProduceResp || binary.BigEndian.Uint64(p) != 2 {
		t.Fatalf("cherry: got type %d %q, want ProduceResp offset 2", typ, p)
	}
	if log2.Len() != 3 {
		t.Fatalf("follower has %d records, want 3 (apple, banana, cherry)", log2.Len())
	}
	for i, m := range []string{"apple", "banana", "cherry"} {
		if got, _ := log2.Read(uint64(i)); string(got) != m {
			t.Fatalf("follower offset %d = %q, want %q", i, got, m)
		}
	}

	// and normal replication keeps working after the catch-up
	if typ, _ := produceTo(t, addr1, "date"); typ != protocol.TypeProduceResp || log2.Len() != 4 {
		t.Fatalf("date: got type %d, follower has %d records, want ProduceResp and 4", typ, log2.Len())
	}
}
