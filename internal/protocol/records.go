package protocol

import (
	"encoding/binary"
	"errors"
)

// Record = one page of a notebook: its offset and its data.
type Record struct {
	Offset uint64
	Data   []byte
}

// AppendRecord adds one record to a FETCH reply: [8-byte offset][4-byte length][data].
func AppendRecord(buf []byte, off uint64, data []byte) []byte {
	var head [12]byte
	binary.BigEndian.PutUint64(head[0:8], off)
	binary.BigEndian.PutUint32(head[8:12], uint32(len(data)))
	buf = append(buf, head[:]...)
	return append(buf, data...)
}

// DecodeRecords splits a FETCH reply back into records.
func DecodeRecords(buf []byte) ([]Record, error) {
	var out []Record
	for len(buf) > 0 {
		if len(buf) < 12 {
			return nil, errors.New("fetch reply: truncated record header")
		}
		off := binary.BigEndian.Uint64(buf[0:8])
		n := int(binary.BigEndian.Uint32(buf[8:12]))
		if len(buf) < 12+n {
			return nil, errors.New("fetch reply: truncated record data")
		}
		out = append(out, Record{Offset: off, Data: buf[12 : 12+n]})
		buf = buf[12+n:]
	}
	return out, nil
}
