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
	TypeProduce     uint16 = 3 // payload = the message
	TypeProduceResp uint16 = 4 // payload = 8-byte offset
	TypeError       uint16 = 5 // payload = error text
	
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