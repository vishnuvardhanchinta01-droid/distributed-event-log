package commitlog

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAppend(t *testing.T) {
	dir := t.TempDir()
	l, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	for i, msg := range []string{"apple", "banana", "cherry"} {
		off, err := l.Append([]byte(msg))
		if err != nil {
			t.Fatal(err)
		}
		if off != uint64(i) {
			t.Fatalf("record %q: got offset %d, want %d", msg, off, i)
		}
	}

	// On disk: [4 len][apple] [4 len][banana] [4 len][cherry]
	info, err := os.Stat(filepath.Join(dir, "00000000.log"))
	if err != nil {
		t.Fatal(err)
	}
	want := int64((4 + 5) + (4 + 6) + (4 + 6))
	if info.Size() != want {
		t.Fatalf("file size = %d, want %d", info.Size(), want)
	}
}
