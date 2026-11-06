package main

import (
	"context"
	"errors"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestYandexPatch(t *testing.T) {
	calls := 0
	fail := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		p, _ := io.ReadAll(r.Body)
		if r.Method != "PATCH" || r.URL.Path != "/bucket/disk/blocks/0000000000000000" || r.Header.Get("Content-Range") != "bytes 10-12/*" || r.ContentLength != 3 || string(p) != "abc" || !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
			t.Errorf("bad PATCH: %s %s %v %q", r.Method, r.URL, r.Header, p)
		}
		if fail {
			http.Error(w, "failure", 500)
			return
		}
		fmt.Fprint(w, "<PatchObjectResult/>")
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{})
	if err != nil {
		t.Fatal(err)
	}
	s := &yandexStore{s3Store: &s3Store{client: client, bucket: "bucket", prefix: "disk"}, httpClient: server.Client(), region: "ru-central1", accessKey: "key", secretKey: "secret"}
	if err := s.Patch(context.Background(), 0, 10, []byte("abc")); err != nil {
		t.Fatal(err)
	}
	fail = true
	if s.Patch(context.Background(), 0, 10, []byte("abc")) == nil {
		t.Fatal("lost error")
	}
	if calls != 2 {
		t.Fatal(calls)
	}
}

type patchMemory struct {
	*memoryStore
	patches int
	offset  int
	payload []byte
}

func (s *patchMemory) Patch(ctx context.Context, id uint64, off int, p []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fail {
		return fmt.Errorf("offline")
	}
	if _, ok := s.data[id]; !ok {
		return errObjectMissing
	}
	s.patches++
	s.offset = off
	s.payload = append([]byte(nil), p...)
	copy(s.data[id][off:], p)
	return nil
}
func TestPatchCache(t *testing.T) {
	ctx := context.Background()
	s := &patchMemory{memoryStore: &memoryStore{data: map[uint64][]byte{0: make([]byte, 512)}}}
	d := &Device{store: s, size: 2048, blockSize: 512, capacity: 4, workers: 1, blocks: map[uint64]*block{}}
	for _, off := range []uint64{10, 12} {
		if err := d.IO(ctx, off, []byte{7}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	s.fail = true
	if d.Flush(ctx) == nil {
		t.Fatal("expected failure")
	}
	s.fail = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s.patches != 2 || s.offset != 12 || len(s.payload) != 1 {
		t.Fatal("range coalescing")
	}
	if err := d.IO(ctx, 513, []byte{9}, true, true); err != nil {
		t.Fatal(err)
	}
	if s.puts != 1 {
		t.Fatal("new object must use PUT")
	}
	if err := d.Discard(ctx, 0, 512, true); err != nil {
		t.Fatal(err)
	}
	if err := d.IO(ctx, 1, []byte{5}, true, true); err != nil {
		t.Fatal(err)
	}
	if s.puts != 2 || s.patches != 2 {
		t.Fatal("deleted object must use PUT")
	}
}

func TestPatchMissingObject(t *testing.T) {
	for _, tc := range []struct {
		code    string
		status  int
		missing bool
	}{{"NoSuchKey", 404, true}, {"NoSuchBucket", 404, false}, {"", 404, false}, {"AccessDenied", 403, false}, {"InternalError", 500, false}} {
		t.Run(tc.code, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != "PATCH" {
					t.Errorf("unexpected method %s", r.Method)
				}
				w.WriteHeader(tc.status)
				fmt.Fprintf(w, "<Error><Code>%s</Code></Error>", tc.code)
			}))
			defer server.Close()
			client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{})
			if err != nil {
				t.Fatal(err)
			}
			s := &yandexStore{s3Store: &s3Store{client: client, bucket: "bucket", prefix: "disk"}, httpClient: server.Client(), region: "ru-central1", accessKey: "key", secretKey: "secret"}
			err = s.Patch(context.Background(), 0, 1, []byte{9})
			if err == nil || errors.Is(err, errObjectMissing) != tc.missing {
				t.Fatal("incorrect fallback classification", err)
			}
		})
	}
}
func TestPatchMissingPreservesRanges(t *testing.T) {
	s := &patchMemory{memoryStore: &memoryStore{data: map[uint64][]byte{}}}
	d := &Device{store: s, size: 512, blockSize: 512, capacity: 1, workers: 1, blocks: map[uint64]*block{}}
	ctx := context.Background()
	for _, off := range []uint64{1, 100} {
		if err := d.IO(ctx, off, []byte{9}, true, false); err != nil {
			t.Fatal(err)
		}
	}
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if s.gets != 0 || s.puts != 1 || len(s.data[0]) != 512 || s.data[0][1] != 9 || s.data[0][100] != 9 || s.data[0][50] != 0 {
		t.Fatal("invalid fallback", s.data)
	}
}
