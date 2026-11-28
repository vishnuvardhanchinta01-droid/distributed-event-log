package broker

import (
	"strings"
	"testing"

	"eventlog/internal/protocol"
)

// A follower must refuse writes; once it wins the chair, it must accept them.
func TestNotLeaderRefuses(t *testing.T) {
	b, addr, store := startBroker(t, "2")
	b.SetLeader(false)

	typ, p := produceTo(t, addr, "apple")
	if typ != protocol.TypeError || !strings.Contains(string(p), "not leader") {
		t.Fatalf("follower: got type %d %q, want a 'not leader' error", typ, p)
	}
	if store.Len() != 0 {
		t.Fatalf("follower stored %d records, want 0", store.Len())
	}

	b.SetLeader(true)
	if typ, p := produceTo(t, addr, "apple"); typ != protocol.TypeProduceResp {
		t.Fatalf("leader: got type %d %q, want ProduceResp", typ, p)
	}
}
