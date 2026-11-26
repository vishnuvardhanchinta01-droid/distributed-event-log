package protocol

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteFrame(&buf, TypePing, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	if err := WriteFrame(&buf, TypePong, nil); err != nil {
		t.Fatal(err)
	}
	typ, p, err := ReadFrame(&buf)
	if err != nil || typ != TypePing || string(p) != "hi" {
		t.Fatalf("frame 1: got %d %q %v", typ, p, err)
	}
	typ, p, err = ReadFrame(&buf)
	if err != nil || typ != TypePong || len(p) != 0 {
		t.Fatalf("frame 2: got %d %q %v", typ, p, err)
	}
}