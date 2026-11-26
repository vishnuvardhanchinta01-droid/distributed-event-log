package commitlog

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// Simulate a crash in the middle of writing the last record by
// cutting bytes off the end of the file, then reopen.
func TestTornWrite(t *testing.T) {
	for _, cut := range []int64{3, 8} { // 3 = inside "cherry", 8 = inside its length header
		dir := t.TempDir()
		l, err := Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range []string{"apple", "banana", "cherry"} {
			l.Append([]byte(m))
		}
		l.Close()

		path := filepath.Join(dir, "00000000.log")
		info, _ := os.Stat(path)
		os.Truncate(path, info.Size()-cut) // 💥 crash mid-write

		l2, err := Open(dir)
		if err != nil {
			t.Fatalf("cut %d: Open failed: %v", cut, err)
		}
		if _, err := l2.Read(2); !errors.Is(err, ErrOffsetOutOfRange) {
			t.Fatalf("cut %d: torn record 2 should be gone, got err %v", cut, err)
		}
		off, err := l2.Append([]byte("date"))
		if err != nil || off != 2 {
			t.Fatalf("cut %d: Append got offset %d, %v; want 2", cut, off, err)
		}
		got, err := l2.Read(2)
		if err != nil || string(got) != "date" {
			t.Fatalf("cut %d: Read(2) = %q, %v; want date", cut, got, err)
		}
		if got, _ := l2.Read(1); string(got) != "banana" {
			t.Fatalf("cut %d: Read(1) = %q; want banana", cut, got)
		}
		l2.Close()
	}
}