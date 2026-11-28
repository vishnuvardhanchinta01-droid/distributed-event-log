package broker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

func fetchFrom(t *testing.T, addr string, p byte, from uint64, max uint32) []protocol.Record {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second))
	req := make([]byte, 13)
	req[0] = p
	binary.BigEndian.PutUint64(req[1:9], from)
	binary.BigEndian.PutUint32(req[9:13], max)
	if err := protocol.WriteFrame(conn, protocol.TypeFetch, req); err != nil {
		t.Fatal(err)
	}
	typ, resp, err := protocol.ReadFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.TypeFetchResp {
		t.Fatalf("fetch: got type %d %q, want FetchResp", typ, resp)
	}
	recs, err := protocol.DecodeRecords(resp)
	if err != nil {
		t.Fatal(err)
	}
	return recs
}

func TestFetch(t *testing.T) {
	_, addr, parts := startBrokerN(t, "1", 2)
	for _, m := range []string{"apple", "banana", "cherry"} {
		parts[1].Append([]byte(m))
	}
	parts[0].Append([]byte("other notebook"))

	got := fetchFrom(t, addr, 1, 1, 10) // notebook 1 from page 1
	if len(got) != 2 || got[0].Offset != 1 || string(got[0].Data) != "banana" || got[1].Offset != 2 || string(got[1].Data) != "cherry" {
		t.Fatalf("fetch p1 from 1 = %v, want [1 banana] [2 cherry]", show(got))
	}
	if got := fetchFrom(t, addr, 1, 0, 1); len(got) != 1 || string(got[0].Data) != "apple" {
		t.Fatalf("fetch p1 from 0 max 1 = %v, want only [0 apple]", show(got))
	}
	if got := fetchFrom(t, addr, 1, 3, 10); len(got) != 0 {
		t.Fatalf("fetch p1 from 3 (the end) = %v, want nothing", show(got))
	}
	if got := fetchFrom(t, addr, 0, 0, 10); len(got) != 1 || string(got[0].Data) != "other notebook" {
		t.Fatalf("fetch p0 = %v, want [0 other notebook]", show(got))
	}
}

func show(rs []protocol.Record) []string {
	var out []string
	for _, r := range rs {
		out = append(out, string(r.Data))
	}
	return out
}
