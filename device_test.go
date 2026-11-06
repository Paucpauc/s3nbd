package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
)

type memoryStore struct {
	mu   sync.Mutex
	data map[uint64][]byte
	puts int
	gets int
	fail bool
}

func (s *memoryStore) Get(_ context.Context, id uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets++
	return append([]byte(nil), s.data[id]...), nil
}
func (s *memoryStore) Put(_ context.Context, id uint64, p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("offline")
	}
	s.puts++
	s.data[id] = append([]byte(nil), p...)
	return nil
}
func TestCoalesceAndRetry(t *testing.T) {
	s := &memoryStore{data: map[uint64][]byte{}}
	d := &Device{store: s, size: 4096, blockSize: 512, capacity: 2, workers: 4, blocks: map[uint64]*block{}}
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if err := d.IO(ctx, 1, []byte{byte(i)}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if s.puts != 0 {
		t.Fatal("early upload")
	}
	s.fail = true
	if d.Flush(ctx) == nil {
		t.Fatal("expected failure")
	}
	if !d.blocks[0].dirty {
		t.Fatal("lost dirty block")
	}
	s.fail = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s.puts != 1 || s.data[0][1] != 9 {
		t.Fatal("coalescing failed")
	}
	if err := d.IO(ctx, 511, []byte{3, 4}, true, true); err != nil {
		t.Fatal(err)
	}
	if s.data[0][511] != 3 || s.data[1][0] != 4 {
		t.Fatal("FUA or cross-block write failed")
	}
	if err := d.IO(ctx, 1024, []byte{5}, true, false); err != nil {
		t.Fatal(err)
	}
	if len(d.blocks) > 2 {
		t.Fatal("cache exceeded")
	}
}

func (s *memoryStore) Delete(_ context.Context, id uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return errors.New("offline")
	}
	delete(s.data, id)
	return nil
}
func TestDiscard(t *testing.T) {
	ctx := context.Background()
	s := &memoryStore{data: map[uint64][]byte{}}
	d := &Device{store: s, size: 2048, blockSize: 512, capacity: 2, workers: 2, blocks: map[uint64]*block{}}
	p := make([]byte, 1024)
	for i := range p {
		p[i] = 7
	}
	if err := d.IO(ctx, 0, p, true, true); err != nil {
		t.Fatal(err)
	}
	if err := d.Discard(ctx, 128, 896, false); err != nil {
		t.Fatal(err)
	}
	s.fail = true
	if d.Flush(ctx) == nil {
		t.Fatal("expected delete failure")
	}
	s.fail = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.data[1]; ok {
		t.Fatal("object not deleted")
	}
	if s.data[0][127] != 7 || s.data[0][128] != 0 {
		t.Fatal("partial discard damaged data")
	}
	// A write after a queued deletion must replace the deletion with a PUT.
	if err := d.Discard(ctx, 0, 512, false); err != nil {
		t.Fatal(err)
	}
	if err := d.IO(ctx, 1, []byte{9}, true, true); err != nil {
		t.Fatal(err)
	}
	if s.data[0][0] != 0 || s.data[0][1] != 9 {
		t.Fatal("write after discard")
	}
	if err := d.Discard(ctx, 0, 512, true); err != nil {
		t.Fatal(err)
	}
	if _, ok := s.data[0]; ok {
		t.Fatal("FUA did not delete")
	}
	if d.Discard(ctx, 2047, 2, false) == nil {
		t.Fatal("accepted invalid range")
	}
}

func TestLazyReads(t *testing.T) {
	for _, patch := range []bool{false, true} {
		t.Run(fmt.Sprint(patch), func(t *testing.T) {
			ctx := context.Background()
			base := &memoryStore{data: map[uint64][]byte{0: bytes.Repeat([]byte{3}, 512)}}
			var store Store = base
			if patch {
				store = &patchMemory{memoryStore: base}
			}
			d := &Device{store: store, size: 4096, blockSize: 512, capacity: 2, workers: 1, blocks: map[uint64]*block{}}
			for off := 0; off < 512; off += 64 {
				if err := d.IO(ctx, uint64(off), make([]byte, 64), true, false); err != nil {
					t.Fatal(err)
				}
			}
			if err := d.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if base.gets != 0 || base.puts != 1 {
				t.Fatalf("full overwrite: GET=%d PUT=%d", base.gets, base.puts)
			}
			// Eviction/restart loses knowledge of the block but must not require GET for writes.
			d.blocks = map[uint64]*block{}
			if err := d.IO(ctx, 10, []byte{7}, true, false); err != nil {
				t.Fatal(err)
			}
			if err := d.IO(ctx, 12, []byte{9}, true, false); err != nil {
				t.Fatal(err)
			}
			p := make([]byte, 1)
			if err := d.IO(ctx, 10, p, false, false); err != nil || p[0] != 7 {
				t.Fatal("read own write", err)
			}
			if base.gets != 0 {
				t.Fatal("write/read known data used GET")
			}
			if err := d.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			want := 1
			if patch {
				want = 0
			}
			if base.gets != want {
				t.Fatal("unexpected GET count", base.gets)
			}
			p = make([]byte, 3)
			if err := d.IO(ctx, 10, p, false, false); err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(p, []byte{7, 0, 9}) {
				t.Fatal("read merge", p)
			}
		})
	}
}
func TestLazyReadPreservesOldData(t *testing.T) {
	ctx := context.Background()
	s := &memoryStore{data: map[uint64][]byte{0: bytes.Repeat([]byte{3}, 512)}}
	d := &Device{store: s, size: 512, blockSize: 512, capacity: 1, workers: 1, blocks: map[uint64]*block{}}
	if err := d.IO(ctx, 10, []byte{7}, true, false); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 3)
	if err := d.IO(ctx, 9, p, false, false); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(p, []byte{3, 7, 3}) {
		t.Fatal(p)
	}
}
