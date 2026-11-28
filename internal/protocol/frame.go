package protocol

import (
	"encoding/binary"
	"errors"
	"io"
)

const (
	TypePing uint16 = 1
	TypePong uint16 = 2
	//new operation => new constants
	TypeProduce     uint16 = 3 // payload = [1-byte partition][message]
	TypeProduceResp uint16 = 4 // payload = 8-byte offset
	TypeError       uint16 = 5 // payload = error text

	// replication (leader -> follower)
	TypeReplicate       uint16 = 6 // payload = [1-byte partition][8-byte offset][message]
	TypeReplicateAck    uint16 = 7 // payload = 8-byte offset the follower stored
	TypeReplicateBehind uint16 = 8 // payload = 8-byte offset the follower expects next ("I'm at page N")

	// reading
	TypeFetch     uint16 = 9  // payload = [1-byte partition][8-byte from offset][4-byte max records]
	TypeFetchResp uint16 = 10 // payload = records, each [8-byte offset][4-byte length][data] (empty = nothing new)

	// backpressure
	TypeBusy uint16 = 11 // payload = text. "Too many orders right now, try again in a moment." Nothing was saved.
)

const MaxFrameSize = 1 << 20 // 1 MiB

var ErrFrameTooLarge = errors.New("frame too large")

// WriteFrame writes [len][type][payload] to w.
func WriteFrame(w io.Writer, typ uint16, payload []byte) error {
	buf := make([]byte, 6)
	binary.BigEndian.PutUint32(buf[0:4], uint32(2+len(payload)))
	binary.BigEndian.PutUint16(buf[4:6], typ)
	data := append(buf, payload...)
	_, err := w.Write(data)
	return err
}

// ReadFrame reads one frame from r.
func ReadFrame(r io.Reader) (uint16, []byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(r, header); err != nil {
		return 0, nil, err
	}
	length := binary.BigEndian.Uint32(header)
	if length < 2 || length > MaxFrameSize {
		return 0, nil, ErrFrameTooLarge
	}
	typBuf := make([]byte, 2) // the box
	if _, err := io.ReadFull(r, typBuf); err != nil {
		return 0, nil, err
	}
	typ := binary.BigEndian.Uint16(typBuf) // the number
	payload := make([]byte, length-2)
	if _, err := io.ReadFull(r, payload); err != nil {
		return 0, nil, err
	}
	return typ, payload, nil
}
