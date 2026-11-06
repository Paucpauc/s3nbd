package main

import (
	"bytes"
	"context"
	"fmt"
	"testing"
	"time"
)

type concurrentReads struct {
	*memoryStore
	started chan uint64
	release chan struct{}
}

func (s *concurrentReads) Get(ctx context.Context, id uint64) ([]byte, error) {
	s.started <- id
	select {
	case <-s.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return s.memoryStore.Get(ctx, id)
}
func TestParallelRead(t *testing.T) {
	s := &concurrentReads{memoryStore: &memoryStore{data: map[uint64][]byte{0: bytes.Repeat([]byte{1}, 512), 1: bytes.Repeat([]byte{2}, 512), 2: bytes.Repeat([]byte{3}, 512)}}, started: make(chan uint64, 3), release: make(chan struct{})}
	d := &Device{store: s, size: 1536, blockSize: 512, capacity: 3, workers: 2, blocks: map[uint64]*block{}}
	// Buffered writes must win over remotely loaded bytes.
	if err := d.IO(context.Background(), 511, []byte{9, 8}, true, false); err != nil {
		t.Fatal(err)
	}
	p := make([]byte, 1024)
	done := make(chan error, 1)
	go func() { done <- d.IO(context.Background(), 256, p, false, false) }()
	defer func() {
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	}()
	for i := 0; i < 2; i++ {
		select {
		case <-s.started:
		case <-time.After(3 * time.Second):
			t.Fatal("GETs not concurrent")
		}
	}
	select {
	case <-s.started:
		t.Fatal("worker limit exceeded")
	default:
	}
	close(s.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if p[0] != 1 || p[255] != 9 || p[256] != 8 || p[257] != 2 || p[768] != 3 {
		t.Fatal("incorrect merged data")
	}
}

type failingReads struct{ *memoryStore }

func (s *failingReads) Get(ctx context.Context, id uint64) ([]byte, error) {
	if id == 1 {
		return nil, fmt.Errorf("offline")
	}
	return s.memoryStore.Get(ctx, id)
}
func TestParallelReadErrorAndSmallCache(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		d := &Device{store: &failingReads{&memoryStore{data: map[uint64][]byte{}}}, size: 1536, blockSize: 512, capacity: capacity, workers: 4, blocks: map[uint64]*block{}}
		if d.IO(context.Background(), 0, make([]byte, 1536), false, false) == nil {
			t.Fatal("lost GET error")
		}
		if len(d.blocks) > capacity {
			t.Fatal("cache limit exceeded")
		}
	}
}

func TestConcurrentRequestsShareLoad(t *testing.T) {
	s := &concurrentReads{memoryStore: &memoryStore{data: map[uint64][]byte{}}, started: make(chan uint64, 8), release: make(chan struct{})}
	d := &Device{store: s, size: 1024, blockSize: 512, capacity: 2, workers: 4, blocks: map[uint64]*block{}}
	done := make(chan error, 3)
	for _, off := range []uint64{0, 0, 512} {
		go func() { done <- d.IO(context.Background(), off, make([]byte, 1), false, false) }()
	}
	defer func() {
		select {
		case <-s.release:
		default:
			close(s.release)
		}
	}()
	ids := map[uint64]bool{}
	for i := 0; i < 2; i++ {
		select {
		case id := <-s.started:
			ids[id] = true
		case <-time.After(3 * time.Second):
			t.Fatal("different READs serialized")
		}
	}
	if len(ids) != 2 {
		t.Fatal("duplicate GET for same block")
	}
	close(s.release)
	for i := 0; i < 3; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if s.gets != 2 {
		t.Fatalf("GET count: %d", s.gets)
	}
}
