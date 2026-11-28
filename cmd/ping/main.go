package main

import (
	"fmt"
	"net"
	"time"

	"eventlog/internal/protocol"
)

// ping sends one PING to addr and prints the PONG.
func ping(addr string) error {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second) // "call" the broker
	if err != nil {
		return err
	}
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(2 * time.Second)) // never wait forever

	if err := protocol.WriteFrame(conn, protocol.TypePing, nil); err != nil {
		return err // inner: did the write fail?
	}
	typ, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	if typ != protocol.TypePong {
		return fmt.Errorf("expected PONG, got type %d", typ)
	}
	fmt.Printf("%s -> PONG from broker %s\n", addr, payload)
	return nil
}

func main() {
	addrs := []string{"localhost:9001", "localhost:9002", "localhost:9003"}
	for _, addr := range addrs { // like Python's "for addr in addrs"
		if err := ping(addr); err != nil {
			fmt.Printf("%s -> FAILED: %v\n", addr, err)
		}
	}
}
