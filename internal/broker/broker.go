package broker

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"net"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

type Broker struct {
	ID    string
	store *commitlog.Log
}

func New(id string, store *commitlog.Log) *Broker {
	return &Broker{ID: id, store: store}
}

// Serve accepts clients forever, one goroutine each.
func (b *Broker) Serve(ln net.Listener) error {
	for { // Go's "while True"
		// TODO 1: conn, err := ln.Accept(); if err, return it
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		// TODO 2: start b.handleConn(conn) as a goroutine
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
			// TODO 1: off, err := b.store.Append(payload)
			off, err :=b.store.Append(payload)
			if err!=nil{
					protocol.WriteFrame(conn, protocol.TypeError, []byte(err.Error()))				
					return
			}
			// TODO 2: if err != nil: send a TypeError frame with []byte(err.Error()), then return
			// TODO 3: resp := make([]byte, 8); binary.BigEndian.PutUint64(resp, off)
			resp := make([]byte, 8); 
			binary.BigEndian.PutUint64(resp, off)
			if err = protocol.WriteFrame(conn,protocol.TypeProduceResp,resp); err!= nil{
				return
			}

		default:
			log.Printf("broker %s: unknown type %d", b.ID, typ)
			return
		}
	}
}
