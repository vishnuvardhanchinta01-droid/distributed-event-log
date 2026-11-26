package broker

import (
	"net"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

// Client A connects and says nothing. Client B must still get a PONG.
func TestTwoClients(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go New("7", newTestLog(t)).Serve(ln)

	quiet, err := net.Dial("tcp", ln.Addr().String()) // client A: silent
	if err != nil {
		t.Fatal(err)
	}
	defer quiet.Close()
	time.Sleep(50 * time.Millisecond) // let the broker accept A first

	b, err := net.Dial("tcp", ln.Addr().String()) // client B
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	b.SetDeadline(time.Now().Add(2 * time.Second)) // give up after 2s

	protocol.WriteFrame(b, protocol.TypePing, nil)
	typ, p, err := protocol.ReadFrame(b)
	if err != nil || typ != protocol.TypePong || string(p) != "7" {
		t.Fatalf("client B got %d %q %v", typ, p, err)
	}
}