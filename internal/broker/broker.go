package broker

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

type Broker struct {
	ID    string
	parts []*commitlog.Log // one notebook per partition (parts[0], parts[1], ...)

	// only set on the leader
	followers []*follower
	replMu    sync.Mutex
	acks      string // "1", "quorum" or "all" (default), see acks.go

	// true while another broker holds the ZooKeeper chair (see SetLeader in leader.go).
	// atomic.Bool = a bool that many goroutines can read/write safely without a mutex.
	notLeader atomic.Bool

	// the in-sync list (ISR), see isr.go
	isrMu       sync.Mutex
	onISRChange func(ids []string) // set by main.go: saves the ISR in ZooKeeper

	// the ovens. A channel used as a box of N tokens (see backpressure.go).
	// nil = no limit.
	slots chan struct{}

	slowDisk time.Duration // extra wait per write (see SetSlowDisk)
}

// New makes a broker with one log per partition: New("1", p0, p1, p2). (Tests often pass just one.)
func New(id string, parts ...*commitlog.Log) *Broker {
	return &Broker{ID: id, parts: parts}
}

// partition splits off the 1-byte partition number at the front of a payload.
func (b *Broker) partition(payload []byte) (int, []byte, error) {
	if len(payload) < 1 {
		return 0, nil, fmt.Errorf("payload too short: no partition byte")
	}
	p := int(payload[0])
	if p >= len(b.parts) {
		return 0, nil, fmt.Errorf("no partition %d (this broker has %d)", p, len(b.parts))
	}
	return p, payload[1:], nil
}

// Serve accepts clients forever, one goroutine each.
func (b *Broker) Serve(ln net.Listener) error {
	for { // Go's "while True"
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go b.handleConn(conn)
	}
}

// handleConn answers frames from ONE client until it disconnects.
func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		typ, payload, err := protocol.ReadFrame(conn)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("broker %s: read error: %v", b.ID, err)
			}
			return
		}

		switch typ {
		case protocol.TypePing:
			if err := protocol.WriteFrame(conn, protocol.TypePong, []byte(b.ID)); err != nil {
				return
			}

		case protocol.TypeProduce:
			// payload = [1-byte partition][message]
			p, msg, err := b.partition(payload)
			if err != nil {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))
				continue
			}
			if !b.IsLeader() {
				protocol.WriteFrame(conn, protocol.TypeError, []byte("not leader"))
				continue
			}
			if !b.tryAcquire() {
				protocol.WriteFrame(conn, protocol.TypeBusy, []byte("busy, try again"))
				continue
			}
			off, err := b.produce(p, msg) // append to partition p + copy to followers (leader.go)
			b.release()
			if err != nil {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))
				return
			}
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, off)
			if err = protocol.WriteFrame(conn, protocol.TypeProduceResp, resp); err != nil {
				return
			}

		case protocol.TypeReplicate:
			// payload = [1-byte partition][8-byte offset][message]
			p, rest, err := b.partition(payload)
			if err != nil {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))
				return
			}
			store := b.parts[p] // the notebook this record belongs in
			payload = rest      // from here on: [8-byte offset][message]
			if len(payload) < 8 {
				protocol.WriteFrame(conn, protocol.TypeError, []byte("replicate: payload too short"))
				return
			}
			want := binary.BigEndian.Uint64(payload[0:8])

			if want > store.Len() {
				next := make([]byte, 8)
				binary.BigEndian.PutUint64(next, store.Len())
				protocol.WriteFrame(conn, protocol.TypeReplicateBehind, next)
				continue
			}

			if want != store.Len() {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(fmt.Sprintf("replicate: out of order, expected offset %d, got %d", store.Len(), want)))
				continue
			}
			off, err := store.Append(payload[8:])
			if err != nil {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))
				return
			}
			resp := make([]byte, 8)
			binary.BigEndian.PutUint64(resp, off)
			if err := protocol.WriteFrame(conn, protocol.TypeReplicateAck, resp); err != nil {
				return
			}
		case protocol.TypeFetch:
			// payload = [1-byte partition][8-byte from offset][4-byte max records]
			p, rest, err := b.partition(payload)
			if err == nil && len(rest) != 12 {
				err = fmt.Errorf("fetch: payload must be 1+8+4 bytes")
			}
			if err == nil && !b.IsLeader() {
				err = fmt.Errorf("not leader")
			}
			var reply []byte
			if err == nil {
				from := binary.BigEndian.Uint64(rest[0:8])
				max := int(binary.BigEndian.Uint32(rest[8:12]))
				reply, err = b.fetch(p, from, max) // fetch.go
			}
			if err != nil {
				protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))
				continue
			}
			if err := protocol.WriteFrame(conn, protocol.TypeFetchResp, reply); err != nil {
				return
			}

		default:
			log.Printf("broker %s: unknown type %d", b.ID, typ)
			return
		}
	}
}
