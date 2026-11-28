package commitlog

import (
	"errors"
	"testing"
)

func TestRead(t *testing.T) {
	l, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	msgs := []string{"apple", "banana", "cherry"}
	for _, m := range msgs {
		if _, err := l.Append([]byte(m)); err != nil {
			t.Fatal(err)
		}
	}
	// read them back out of order on purpose
	for _, i := range []int{2, 0, 1} {
		got, err := l.Read(uint64(i))
		if err != nil || string(got) != msgs[i] {
			t.Fatalf("Read(%d) = %q, %v; want %q", i, got, err, msgs[i])
		}
	}
	// an offset that doesn't exist yet
	if _, err := l.Read(3); !errors.Is(err, ErrOffsetOutOfRange) {
		t.Fatalf("Read(3): want ErrOffsetOutOfRange, got %v", err)
	}
}
