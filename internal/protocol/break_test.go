package protocol

import (
	"bytes"
	"errors"
	"testing"
	"testing/iotest"
)

// TCP can hand you bytes in tiny pieces.
// OneByteReader pretends to be a very slow network: 1 byte per Read.
func TestSplitBytes(t *testing.T) {
	var buf bytes.Buffer
	WriteFrame(&buf, TypePing, []byte("hello"))
	slow := iotest.OneByteReader(&buf)
	typ, p, err := ReadFrame(slow)
	if err != nil || typ != TypePing || string(p) != "hello" {
		t.Fatalf("got %d %q %v", typ, p, err)
	}
}

// A garbage length must be rejected, not allocated.
func TestGarbageLength(t *testing.T) {
	bad := bytes.NewReader([]byte{255, 255, 255, 255, 0, 1})
	_, _, err := ReadFrame(bad)
	if !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("expected ErrFrameTooLarge, got %v", err)
	}
}