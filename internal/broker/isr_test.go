package broker

import (
	"fmt"
	"testing"

	"eventlog/internal/protocol"
)

// A follower that fails is dropped from the ISR, so the NEXT write (acks=all) succeeds without it.
func TestISRShrinks(t *testing.T) {
	_, addr2, _ := startBroker(t, "2")
	leader, addr1, _ := startBroker(t, "1")
	leader.SetFollowers(map[string]string{"2": addr2, "3": deadAddr}, map[string]bool{"2": true, "3": true})

	if typ, _ := produceTo(t, addr1, "apple"); typ != protocol.TypeError {
		t.Fatalf("apple: got type %d, want an error (3 was in the ISR and is dead)", typ)
	}
	if got := fmt.Sprint(leader.ISR()); got != "[1 2]" {
		t.Fatalf("ISR after the failure = %s, want [1 2]", got)
	}
	if typ, p := produceTo(t, addr1, "banana"); typ != protocol.TypeProduceResp {
		t.Fatalf("banana: got type %d %q, want saved (dead 3 is no longer in the ISR)", typ, p)
	}
}

// A new follower starts OUTSIDE the ISR and joins once it has caught up.
func TestISRGrows(t *testing.T) {
	_, addr2, log2 := startBroker(t, "2")
	leader, addr1, log1 := startBroker(t, "1")
	log1.Append([]byte("apple")) // the newcomer missed this one
	log1.Append([]byte("banana"))

	var published []string
	leader.OnISRChange(func(ids []string) { published = ids })
	leader.SetFollowers(map[string]string{"2": addr2}, nil) // nil: nobody starts in the ISR

	if got := fmt.Sprint(leader.ISR()); got != "[1]" {
		t.Fatalf("ISR before catch-up = %s, want [1]", got)
	}
	if typ, _ := produceTo(t, addr1, "cherry"); typ != protocol.TypeProduceResp {
		t.Fatalf("cherry: got type %d, want saved", typ)
	}
	// replication to 2 finishes in the background; the next produce waits for it (replMu)
	produceTo(t, addr1, "date")
	if got := fmt.Sprint(leader.ISR()); got != "[1 2]" {
		t.Fatalf("ISR after catch-up = %s, want [1 2]", got)
	}
	if fmt.Sprint(published) != "[1 2]" {
		t.Fatalf("ZooKeeper was told %v, want [1 2]", published)
	}
	if log2.Len() != 4 {
		t.Fatalf("follower has %d records, want 4", log2.Len())
	}
}
