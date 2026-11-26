package commitlog

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"errors"
)

type Log struct {
	mu      sync.Mutex
	file    *os.File
	offsets []int64 // offsets[i] = byte position where record i starts
	size    int64   // file size = where the next record goes
}

// rebuild scans the file from the start and refills l.offsets.
func (l *Log) rebuild() error {
	var pos int64            // starts at 0
	header := make([]byte, 4)
	// TODO: loop while pos < l.size   (Go's while: `for pos < l.size { ... }`)
	//   1. ReadAt 4 bytes at pos into header (check the error!)
	//   2. n := decode the length
	//   3. append pos to l.offsets
	//   4. pos += 4 + int64(n)
	for pos < l.size{
		if pos+4 > l.size{
			break
		}
		if _, err := l.file.ReadAt(header, pos); err != nil {
		return err
		}
		n := binary.BigEndian.Uint32(header)
		if pos + 4 + int64(n) > l.size{
			break
		}
		l.offsets = append(l.offsets, pos)
		pos += 4 + int64(n)
	}
	if pos < l.size {   // leftover broken bytes at the end
		if err := l.file.Truncate(pos); err != nil {
			return err
		}
		l.size = pos
	}
	return nil
}

// Open opens (or creates) the log file inside dir. (Given: setup code.)
func Open(dir string) (*Log, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "00000000.log"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644) // create if missing, read+write, always write at end
	if err != nil {
		return nil, err
	}
	info, err := f.Stat() // how big is the file already?
	if err != nil {
		f.Close()
		return nil, err
	}
	l := &Log{file: f, size: info.Size()}
	if err := l.rebuild(); err != nil {
		f.Close()
		return nil, err
	}
	return l, nil
}

// Append writes one record and returns its offset.
func (l *Log) Append(data []byte) (uint64, error) {
	// TODO 1: lock the mutex (and defer the unlock)
	l.mu.Lock()
	defer l.mu.Unlock()
	// TODO 2: build buf = [4-byte length of data][data]
	//         (just like WriteFrame, but without the type field)
	buffer :=make([]byte,4)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(len(data)))
	buf := append(buffer,data...)
	// TODO 3: write buf to l.file; return the error if it fails
	if _,err := l.file.Write(buf); err !=nil{
		return 0,err
	}
	// TODO 4: the new record's offset = how many records we have so far = len(l.offsets)
	currlen := len(l.offsets)
	// TODO 5: remember where it started: append l.size to l.offsets
	l.offsets = append(l.offsets, l.size)
	// TODO 6: grow l.size by len(buf)
	l.size += int64(len(buf))
	// TODO 7: return the offset, nil
	return uint64(currlen), nil
}

var ErrOffsetOutOfRange = errors.New("offset out of range")

// Read returns the record stored at offset.
func (l *Log) Read(offset uint64) ([]byte, error) {
	// TODO 1: lock + defer unlock (why do we need the lock for reading too?)
	l.mu.Lock()
	defer l.mu.Unlock()
	// TODO 2: if offset >= number of records, return nil, ErrOffsetOutOfRange
	//         (careful: len() is int, offset is uint64. Convert one of them)
	if offset>=uint64(len(l.offsets)){
		return nil,ErrOffsetOutOfRange
	}
	// TODO 3: pos := the byte position of this record (from l.offsets)
	pos := l.offsets[offset]
	// TODO 4: read 4 boxes at pos with l.file.ReadAt → decode the length n
	length :=make([]byte,4)
	if _, err := l.file.ReadAt(length, pos); err != nil {
	return nil, err
	}
	// TODO 5: make a box of n bytes, ReadAt it at pos+4
	n := binary.BigEndian.Uint32(length)
	data := make([]byte,n)
	if _, err := l.file.ReadAt(data, pos+4); err != nil {
	return nil, err
	}
	// TODO 6: return the data, nil
	return data,nil
}

func (l *Log) Close() error { return l.file.Close() }