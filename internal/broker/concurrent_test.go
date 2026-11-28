package broker

import (
	"encoding/binary"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"eventlog/internal/protocol"
)

// 20 producers send 50 messages each, all at the same time.
// Every message must get its own unique offset, and all 1000 must be stored.
func TestManyProducers(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	store := newTestLog(t)
	go New("7", store).Serve(ln)

	const producers, perProducer = 20, 50
	var mu sync.Mutex
	seen := map[uint64]string{} // offset -> message (to spot duplicates)

	var wg sync.WaitGroup // waits for all goroutines to finish (like join())
	for p := 0; p < producers; p++ {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", ln.Addr().String())
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			conn.SetDeadline(time.Now().Add(5 * time.Second))
			for i := 0; i < perProducer; i++ {
				msg := fmt.Sprintf("p%d-m%d", p, i)
				if err := protocol.WriteFrame(conn, protocol.TypeProduce, append([]byte{0}, msg...)); err != nil {
					t.Error(err)
					return
				}
				typ, payload, err := protocol.ReadFrame(conn)
				if err != nil || typ != protocol.TypeProduceResp {
					t.Errorf("%s: type %d, err %v", msg, typ, err)
					return
				}
				off := binary.BigEndian.Uint64(payload)
				mu.Lock()
				if other, dup := seen[off]; dup {
					t.Errorf("offset %d given to both %s and %s", off, other, msg)
				}
				seen[off] = msg
				mu.Unlock()
			}
		}(p)
	}
	wg.Wait()

	if len(seen) != producers*perProducer {
		t.Fatalf("got %d unique offsets, want %d", len(seen), producers*perProducer)
	}
	// every offset must read back as exactly the message it was promised to
	for off, msg := range seen {
		got, err := store.Read(off)
		if err != nil || string(got) != msg {
			t.Fatalf("Read(%d) = %q, %v; want %q", off, got, err, msg)
		}
	}
}
