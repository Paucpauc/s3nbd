package main

import (
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"
)

func TestNBDWriteFlushRead(t *testing.T) {
	s := &memoryStore{data: map[uint64][]byte{}}
	d := &Device{store: s, size: 64 << 20, blockSize: 1 << 20, capacity: 2, workers: 2, blocks: map[uint64]*block{}}
	a, c := net.Pipe()
	done := make(chan struct{})
	go func() { serve(a, map[string]*Device{"disk": d}); close(done) }()
	defer func() { c.Close(); <-done }()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	h := make([]byte, 18)
	if _, err := io.ReadFull(c, h); err != nil {
		t.Fatal(err)
	}
	send(c, uint32(3), optMagic, uint32(1), uint32(4), []byte("disk"))
	h = make([]byte, 10)
	if _, err := io.ReadFull(c, h); err != nil {
		t.Fatal(err)
	}
	if be.Uint64(h) != 64<<20 {
		t.Fatal("size")
	}
	request := func(cmd uint16, n uint32, p []byte) {
		t.Helper()
		if err := send(c, uint32(0x25609513), uint16(0), cmd, uint64(123), uint64(0), n, p); err != nil {
			t.Fatal(err)
		}
		var r struct {
			Magic, Error uint32
			Handle       uint64
		}
		if err := binary.Read(c, be, &r); err != nil {
			t.Fatal(err)
		}
		if r.Error != 0 || r.Handle != 123 || r.Magic != 0x67446698 {
			t.Fatalf("reply: %+v", r)
		}
	}
	if be.Uint16(h[8:])&32 == 0 {
		t.Fatal("TRIM not advertised")
	}
	request(1, 3, []byte("abc"))
	request(3, 0, nil)
	s.mu.Lock()
	ok := string(s.data[0][:3]) == "abc"
	s.mu.Unlock()
	if !ok {
		t.Fatal("flush did not persist")
	}
	request(0, 3, nil)
	p := make([]byte, 3)
	if _, err := io.ReadFull(c, p); err != nil || string(p) != "abc" {
		t.Fatalf("read %q: %v", p, err)
	}
	request(4, 33<<20, nil)
	request(3, 0, nil)
	s.mu.Lock()
	_, exists := s.data[0]
	s.mu.Unlock()
	if exists {
		t.Fatal("NBD TRIM did not delete")
	}
	request(0, 3, nil)
	if _, err := io.ReadFull(c, p); err != nil {
		t.Fatal(err)
	}
	for _, b := range p {
		if b != 0 {
			t.Fatal("stale cache after trim")
		}
	}

}

func TestNBDConcurrentReadBarrier(t *testing.T) {
	s := &concurrentReads{memoryStore: &memoryStore{data: map[uint64][]byte{}}, started: make(chan uint64, 4), release: make(chan struct{})}
	d := &Device{store: s, size: 1024, blockSize: 512, capacity: 2, workers: 2, blocks: map[uint64]*block{}}
	a, c := net.Pipe()
	done := make(chan struct{})
	go func() { serve(a, map[string]*Device{"disk": d}); close(done) }()
	defer func() {
		select {
		case <-s.release:
		default:
			close(s.release)
		}
		c.Close()
		<-done
	}()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	h := make([]byte, 18)
	if _, err := io.ReadFull(c, h); err != nil {
		t.Fatal(err)
	}
	send(c, uint32(3), optMagic, uint32(1), uint32(4), []byte("disk"))
	if _, err := io.ReadFull(c, make([]byte, 10)); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < 2; i++ {
		if err := send(c, uint32(0x25609513), uint16(0), uint16(0), i, i*512, uint32(1)); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 2; i++ {
		select {
		case <-s.started:
		case <-time.After(3 * time.Second):
			t.Fatal("NBD READs serialized")
		}
	}
	// WRITE must not overtake either pending READ.
	sent := make(chan error, 1)
	go func() {
		sent <- send(c, uint32(0x25609513), uint16(1), uint16(1), uint64(2), uint64(0), uint32(1), []byte{9})
	}()
	close(s.release)
	seen := map[uint64]bool{}
	for i := 0; i < 3; i++ {
		var r struct {
			Magic, Error uint32
			Handle       uint64
		}
		if err := binary.Read(c, be, &r); err != nil {
			t.Fatal(err)
		}
		if r.Error != 0 {
			t.Fatal(r)
		}
		if i < 2 {
			if r.Handle > 1 || seen[r.Handle] {
				t.Fatal("bad read ordering", r)
			}
			seen[r.Handle] = true
			p := make([]byte, 1)
			if _, err := io.ReadFull(c, p); err != nil || p[0] != 0 {
				t.Fatal("write overtook read")
			}
		} else if r.Handle != 2 {
			t.Fatal("missing write reply")
		}
	}
	if err := <-sent; err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	v := s.data[0][0]
	s.mu.Unlock()
	if v != 9 {
		t.Fatal("FUA not persisted")
	}
}

// Exercise both negotiation paths against the same live export, including
// cache coherence, cross-connection FLUSH, and disconnecting only one socket.
func TestNBDMultiConnection(t *testing.T) {
	s := &memoryStore{data: map[uint64][]byte{}}
	d := &Device{store: s, size: 1024, blockSize: 512, capacity: 2, workers: 2, blocks: map[uint64]*block{}}
	connect := func(opt uint32) net.Conn {
		t.Helper()
		a, c := net.Pipe()
		done := make(chan struct{})
		go func() { serve(a, map[string]*Device{"disk": d}); close(done) }()
		t.Cleanup(func() { c.Close(); <-done })
		c.SetDeadline(time.Now().Add(5 * time.Second))
		if _, err := io.ReadFull(c, make([]byte, 18)); err != nil {
			t.Fatal(err)
		}
		p := []byte("disk")
		if opt != 1 {
			p = make([]byte, 10)
			be.PutUint32(p, 4)
			copy(p[4:], "disk")
		}
		if err := send(c, uint32(3), optMagic, opt, uint32(len(p)), p); err != nil {
			t.Fatal(err)
		}
		var exportFlags uint16
		if opt == 1 {
			h := make([]byte, 10)
			if _, err := io.ReadFull(c, h); err != nil {
				t.Fatal(err)
			}
			exportFlags = be.Uint16(h[8:])
		} else {
			for _, kind := range []uint32{3, 1} {
				var h struct {
					Magic             uint64
					Opt, Kind, Length uint32
				}
				if err := binary.Read(c, be, &h); err != nil {
					t.Fatal(err)
				}
				if h.Magic != 0x3e889045565a9 || h.Opt != opt || h.Kind != kind {
					t.Fatalf("option reply: %+v", h)
				}
				q := make([]byte, h.Length)
				if _, err := io.ReadFull(c, q); err != nil {
					t.Fatal(err)
				}
				if kind == 3 {
					if len(q) != 12 {
						t.Fatal("export info length")
					}
					exportFlags = be.Uint16(q[10:])
				}
			}
		}
		if exportFlags&256 == 0 {
			t.Fatal("CAN_MULTI_CONN missing")
		}
		return c
	}
	a := connect(7)
	b := connect(1)
	request := func(c net.Conn, cmd, flags uint16, p []byte) []byte {
		t.Helper()
		var payload []byte
		if cmd == 1 {
			payload = p
		}
		if err := send(c, uint32(0x25609513), flags, cmd, uint64(17), uint64(0), uint32(len(p)), payload); err != nil {
			t.Fatal(err)
		}
		var r struct {
			Magic, Error uint32
			Handle       uint64
		}
		if err := binary.Read(c, be, &r); err != nil {
			t.Fatal(err)
		}
		if r.Magic != 0x67446698 || r.Error != 0 || r.Handle != 17 {
			t.Fatalf("reply: %+v", r)
		}
		if cmd == 0 {
			out := make([]byte, len(p))
			if _, err := io.ReadFull(c, out); err != nil {
				t.Fatal(err)
			}
			return out
		}
		return nil
	}
	request(a, 1, 0, []byte("abc"))
	if got := request(b, 0, 0, make([]byte, 3)); string(got) != "abc" {
		t.Fatalf("shared cache: %q", got)
	}
	request(b, 3, 0, nil)
	s.mu.Lock()
	persisted := string(s.data[0][:3])
	s.mu.Unlock()
	if persisted != "abc" {
		t.Fatal("cross-connection flush")
	}
	request(b, 1, 1, []byte("xyz"))
	if got := request(a, 0, 0, make([]byte, 3)); string(got) != "xyz" {
		t.Fatalf("FUA visibility: %q", got)
	}
	a.Close()
	request(b, 4, 0, make([]byte, 3))
	if got := request(b, 0, 0, make([]byte, 3)); string(got) != "\x00\x00\x00" {
		t.Fatalf("trim: %q", got)
	}
}
