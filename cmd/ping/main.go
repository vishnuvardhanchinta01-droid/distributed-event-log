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

	// TODO 1: send a PING frame on conn (empty payload: nil). Return the error if it fails.
	if err := protocol.WriteFrame(conn, protocol.TypePing, nil); err != nil {
		return err // inner: did the write fail?
	}
	// TODO 2: read a frame from conn. Return the error if it fails.
	typ, payload, err := protocol.ReadFrame(conn)
	if err != nil {
		return err
	}
	// TODO 3: if the type isn't TypePong, return an error:
	//         fmt.Errorf("expected PONG, got type %d", typ)
	if typ != protocol.TypePong {
		return fmt.Errorf("expected PONG, got type %d", typ)
	}
	// TODO 4: print it:  fmt.Printf("%s -> PONG from broker %s\n", addr, payload)
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
