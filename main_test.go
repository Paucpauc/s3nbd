package main

import (
	"context"
	"strings"
	"testing"
)

func TestFinalFlushContinuesAfterErrors(t *testing.T) {
	ctx := context.Background()
	devices := map[string]*Device{}
	stores := map[string]*memoryStore{}
	// Two failures ensure a fail-fast loop cannot visit every device,
	// regardless of map iteration order.
	for _, name := range []string{"offline-a", "healthy", "offline-b"} {
		s := &memoryStore{data: map[uint64][]byte{}, fail: name != "healthy"}
		d := &Device{store: s, size: 512, blockSize: 512, capacity: 1, workers: 1, blocks: map[uint64]*block{}}
		if err := d.IO(ctx, 0, []byte{42}, true, false); err != nil {
			t.Fatal(err)
		}
		stores[name], devices[name] = s, d
	}
	err := flushDevices(ctx, devices)
	if err == nil {
		t.Fatal("lost flush errors")
	}
	for _, name := range []string{"offline-a", "offline-b"} {
		if !strings.Contains(err.Error(), "final flush "+name+":") {
			t.Fatalf("missing error for %s: %v", name, err)
		}
		if devices[name].dirtyBlocks.Load() != 1 {
			t.Fatalf("failed flush lost dirty data on %s", name)
		}
		stores[name].fail = false
	}
	if p := stores["healthy"].data[0]; len(p) != 512 || p[0] != 42 {
		t.Fatal("healthy export was not persisted")
	}
	if devices["healthy"].dirtyBlocks.Load() != 0 {
		t.Fatal("healthy export remains dirty")
	}
	if err := flushDevices(ctx, devices); err != nil {
		t.Fatal(err)
	}
	for name, s := range stores {
		if s.puts != 1 || len(s.data[0]) != 512 || s.data[0][0] != 42 {
			t.Fatalf("incorrect retry on %s", name)
		}
	}
}
