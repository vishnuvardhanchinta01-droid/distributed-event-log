// Package commitlog stores records in an append-only file and rebuilds its in-memory index on startup.
package commitlog

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
)

// Log is an append-only file of records [4-byte length][data]; a record's offset is its position in the file (0, 1, 2, ...).
type Log struct {
	mu      sync.Mutex
	file    *os.File
	offsets []int64 // offsets[i] = byte position where record i starts
	size    int64   // file size = where the next record goes
}

// rebuild scans the file from the start and refills l.offsets.
func (l *Log) rebuild() error {
	var pos int64 // starts at 0
	header := make([]byte, 4)
	for pos < l.size {
		if pos+4 > l.size {
			break
		}
		if _, err := l.file.ReadAt(header, pos); err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(header)
		if pos+4+int64(n) > l.size {
			break
		}
		l.offsets = append(l.offsets, pos)
		pos += 4 + int64(n)
	}
	if pos < l.size { // leftover broken bytes at the end
		if err := l.file.Truncate(pos); err != nil {
			return err
		}
		l.size = pos
	}
	return nil
}

// Open opens (or creates) the log file inside dir.
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
	l.mu.Lock()
	defer l.mu.Unlock()
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer[0:4], uint32(len(data)))
	buf := append(buffer, data...)
	if _, err := l.file.Write(buf); err != nil {
		return 0, err
	}
	currlen := len(l.offsets)
	l.offsets = append(l.offsets, l.size)
	l.size += int64(len(buf))
	return uint64(currlen), nil
}

var ErrOffsetOutOfRange = errors.New("offset out of range")

// Read returns the record stored at offset.
func (l *Log) Read(offset uint64) ([]byte, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset >= uint64(len(l.offsets)) {
		return nil, ErrOffsetOutOfRange
	}
	pos := l.offsets[offset]
	length := make([]byte, 4)
	if _, err := l.file.ReadAt(length, pos); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(length)
	data := make([]byte, n)
	if _, err := l.file.ReadAt(data, pos+4); err != nil {
		return nil, err
	}
	return data, nil
}

// Len returns how many records the log has (= the next offset to be written).
func (l *Log) Len() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return uint64(len(l.offsets))
}

func (l *Log) Close() error { return l.file.Close() }
