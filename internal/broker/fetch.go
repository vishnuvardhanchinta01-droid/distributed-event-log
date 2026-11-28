package broker

import (
	"errors"

	"eventlog/internal/commitlog"
	"eventlog/internal/protocol"
)

// fetch = "show me notebook p from page `from`, at most `max` pages".
// It returns the pages packed into one FETCH reply.
func (b *Broker) fetch(p int, from uint64, max int) ([]byte, error) {
	store := b.parts[p]
	var reply []byte

	for off := from; off < from+uint64(max); off++ {
		data, err := store.Read(off)
		if errors.Is(err, commitlog.ErrOffsetOutOfRange) {
			break
		}
		if err != nil {
			return nil, err
		}
		reply = protocol.AppendRecord(reply, off, data)
	}
	return reply, nil
}
