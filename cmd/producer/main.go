package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"eventlog/internal/protocol"
)

// produce sends one message to the broker and returns the offset it was stored at.
func produce(conn net.Conn, msg string) (uint64, error) {
	// TODO 1: send a TypeProduce frame with []byte(msg). Return 0, err if it fails.
	if err := protocol.WriteFrame(conn, protocol.TypeProduce, []byte(msg)); err != nil {
		return 0, err
	}
	// TODO 2: read the reply frame (typ, payload, err). Return 0, err if it fails.
	typ, payload ,err := protocol.ReadFrame(conn); 
	if err!=nil{
		return 0,err
	}
		// TODO 3: switch on typ:
		//   case protocol.TypeProduceResp: return binary.BigEndian.Uint64(payload), nil
		//   case protocol.TypeError:       return 0, fmt.Errorf("broker error: %s", payload)
		//   default:                       return 0, fmt.Errorf("unexpected reply type %d", typ)
	switch typ {
	case  protocol.TypeProduceResp:
		return binary.BigEndian.Uint64(payload), nil
	case protocol.TypeError:
		return 0, fmt.Errorf("broker error: %s", payload)
	default: 
		return 0, fmt.Errorf("unexpected reply type %d", typ)
	}
}

// Usage: go run ./cmd/producer --addr localhost:9001 apple banana cherry
func main() {
	addr := flag.String("addr", "localhost:9001", "broker address")
	flag.Parse()
	msgs := flag.Args() // everything after the flags = the messages
	if len(msgs) == 0 {
		log.Fatal("usage: producer [--addr host:port] msg1 msg2 ...")
	}

	conn, err := net.DialTimeout("tcp", *addr, 2*time.Second)
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer conn.Close()

	for _, m := range msgs {
		conn.SetDeadline(time.Now().Add(2 * time.Second))
		off, err := produce(conn, m)
		if err != nil {
			log.Fatalf("produce %q: %v", m, err)
		}
		fmt.Printf("%q -> offset %d\n", m, off)
	}
}
