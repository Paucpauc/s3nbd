package main

import (
	"context"
	"encoding/xml"
	"fmt"
	"github.com/minio/minio-go/v7"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

type batchMemory struct {
	*memoryStore
	batchMu sync.Mutex
	batches [][]uint64
	fail    bool
}

func (s *batchMemory) DeleteBatch(ctx context.Context, ids []uint64) map[uint64]error {
	s.batchMu.Lock()
	defer s.batchMu.Unlock()
	s.batches = append(s.batches, append([]uint64(nil), ids...))
	out := map[uint64]error{}
	for _, id := range ids {
		if s.fail && id == 1 {
			out[id] = fmt.Errorf("denied")
		} else {
			out[id] = s.Delete(ctx, id)
		}
	}
	return out
}
func TestBatchDeleteRetry(t *testing.T) {
	s := &batchMemory{memoryStore: &memoryStore{data: map[uint64][]byte{}}, fail: true}
	d := &Device{store: s, size: 1024 * 512, blockSize: 512, capacity: 1024, workers: 2, blocks: map[uint64]*block{}}
	ctx := context.Background()
	if err := d.Discard(ctx, 0, 1024*512, true); err == nil {
		t.Fatal("expected partial failure")
	}
	if len(s.batches) != 2 {
		t.Fatal("batch split", len(s.batches))
	}
	for _, ids := range s.batches {
		if len(ids) > 1000 {
			t.Fatal("too many keys")
		}
	}
	for id, b := range d.blocks {
		if b.dirty != (id == 1) {
			t.Fatal("incorrect dirty state", id)
		}
	}
	s.fail = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if len(s.batches) != 3 || len(s.batches[2]) != 1 || s.batches[2][0] != 1 {
		t.Fatal("retried successful deletions")
	}
}
func TestS3MultiDelete(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.Method != "POST" || !r.URL.Query().Has("delete") {
			t.Errorf("not multi-delete: %s %s", r.Method, r.URL)
		}
		var req struct {
			Objects []struct{ Key string } `xml:"Object"`
		}
		if err := xml.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Error(err)
		}
		if len(req.Objects) != 3 {
			t.Errorf("keys: %d", len(req.Objects))
		}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprint(w, `<DeleteResult><Deleted><Key>disk/blocks/0000000000000000</Key></Deleted><Error><Key>disk/blocks/0000000000000001</Key><Code>AccessDenied</Code><Message>denied</Message></Error></DeleteResult>`)
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	s := &s3Store{client: client, bucket: "bucket", prefix: "disk"}
	results := s.DeleteBatch(context.Background(), []uint64{0, 1, 2})
	if results[0] != nil || results[1] == nil || results[2] == nil || calls != 1 {
		t.Fatalf("results: %v, calls %d", results, calls)
	}
}
