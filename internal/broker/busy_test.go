package broker

import (
	"net"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

// One oven only. Order A is stuck in the kitchen (the follower takes 1s),
// so order B must get BUSY at once, not wait 1 second.
func TestBusyWhenFull(t *testing.T) {
	slow := slowFollower(t, time.Second)
	leader, addr, _ := startBroker(t, "1")
	if err := leader.SetAcks("all"); err != nil {
		t.Fatal(err)
	}
	leader.AddFollowers(slow)
	leader.SetMaxInFlight(1)

	go func() { // order A: takes the only oven for ~1s
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			return
		}
		defer conn.Close()
		protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, "A"...))
		protocol.ReadFrame(conn)
	}()
	time.Sleep(200 * time.Millisecond) // let A get into the kitchen

	start := time.Now()
	typ, p := produceTo(t, addr, "B")
	if typ != protocol.TypeBusy {
		t.Fatalf("B: got type %d %q, want TypeBusy (11)", typ, p)
	}
	if took := time.Since(start); took > 300*time.Millisecond {
		t.Fatalf("B took %v: it waited in line instead of getting BUSY right away", took)
	}

	// After A finishes, the oven is free again: C must be saved.
	time.Sleep(1200 * time.Millisecond)
	if typ, p := produceTo(t, addr, "C"); typ != protocol.TypeProduceResp {
		t.Fatalf("C: got type %d %q, want ProduceResp (did release() give the oven back?)", typ, p)
	}
}

// BUSY must not close the connection: the producer retries on the same one.
func TestBusyKeepsConnection(t *testing.T) {
	leader, addr, _ := startBroker(t, "1")
	leader.SetMaxInFlight(1)
	leader.slots <- struct{}{} // pretend the only oven is taken

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, "X"...))
	if typ, _, err := protocol.ReadFrame(conn); err != nil || typ != protocol.TypeBusy {
		t.Fatalf("first try: type %d err %v, want TypeBusy", typ, err)
	}

	<-leader.slots // the oven frees up
	protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, "X"...))
	if typ, _, err := protocol.ReadFrame(conn); err != nil || typ != protocol.TypeProduceResp {
		t.Fatalf("retry on same connection: type %d err %v, want ProduceResp", typ, err)
	}
}
