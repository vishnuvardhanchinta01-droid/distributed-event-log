package broker

import (
	"net"
	"testing"

	"eventlog/internal/protocol"
)

func TestPingPong(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0") // :0 = any free port
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	b := New("7", newTestLog(t))
	go b.Serve(ln)

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	for i := 0; i < 2; i++ { // two pings on the same connection
		if err := protocol.WriteFrame(conn, protocol.TypePing, nil); err != nil {
			t.Fatal(err)
		}
		typ, p, err := protocol.ReadFrame(conn)
		if err != nil || typ != protocol.TypePong || string(p) != "7" {
			t.Fatalf("ping %d: got %d %q %v", i, typ, p, err)
		}
	}
}
