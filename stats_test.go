package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestTransportStats(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.Copy(io.Discard, r.Body); io.WriteString(w, "reply") }))
	defer server.Close()
	stats := &statsTransport{base: http.DefaultTransport}
	client := &http.Client{Transport: stats}
	for _, method := range []string{"PATCH", "POST"} {
		req, _ := http.NewRequest(method, server.URL+"/?delete", strings.NewReader("abc"))
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
	counts := stats.snapshot()
	for _, method := range []string{"PATCH", "POST(DeleteObjects)"} {
		if counts[method] != (methodStats{1, 3, 5}) {
			t.Fatal(method, counts)
		}
	}
	if len(stats.snapshot()) != 0 {
		t.Fatal("interval not reset")
	}
}
func TestDirtyStats(t *testing.T) {
	ctx := context.Background()
	s := &memoryStore{data: map[uint64][]byte{}}
	d := &Device{store: s, size: 1024, blockSize: 512, capacity: 2, workers: 2, blocks: map[uint64]*block{}}
	for i := 0; i < 2; i++ {
		if err := d.IO(ctx, 0, []byte{1}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Discard(ctx, 512, 512, false); err != nil {
		t.Fatal(err)
	}
	if d.dirtyBlocks.Load() != 2 {
		t.Fatal("dirty count")
	}
	s.fail = true
	if d.Flush(ctx) == nil {
		t.Fatal("expected failure")
	}
	if d.dirtyBlocks.Load() != 2 {
		t.Fatal("lost failed dirty count")
	}
	s.fail = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if d.dirtyBlocks.Load() != 0 {
		t.Fatal("not cleared")
	}
}
