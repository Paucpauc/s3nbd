package main

import (
	"context"
	"github.com/minio/minio-go/v7"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestZeroPutDeletesAndRetries(t *testing.T) {
	var methods []string
	deny := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		switch r.Method {
		case "DELETE":
			if deny {
				w.WriteHeader(403)
				io.WriteString(w, "<Error><Code>AccessDenied</Code></Error>")
				return
			}
			w.WriteHeader(204)
		case "PUT":
			io.Copy(io.Discard, r.Body)
			w.Header().Set("ETag", `"abc"`)
		default:
			t.Errorf("unexpected %s", r.Method)
			w.WriteHeader(400)
		}
	}))
	defer server.Close()
	client, err := minio.New(strings.TrimPrefix(server.URL, "http://"), &minio.Options{Region: "us-east-1", BucketLookup: minio.BucketLookupPath})
	if err != nil {
		t.Fatal(err)
	}
	s := &s3Store{client: client, bucket: "bucket", prefix: "disk", blockSize: 512}
	d := &Device{store: s, size: 512, blockSize: 512, capacity: 1, workers: 1, blocks: map[uint64]*block{}}
	ctx := context.Background()
	if d.IO(ctx, 0, make([]byte, 512), true, true) == nil {
		t.Fatal("delete error lost")
	}
	if d.dirtyBlocks.Load() != 1 {
		t.Fatal("failed delete lost dirty data")
	}
	deny = false
	if err := d.Flush(ctx); err != nil {
		t.Fatal(err)
	}
	if d.dirtyBlocks.Load() != 0 {
		t.Fatal("successful delete not clean")
	}
	p := make([]byte, 512)
	if err := d.IO(ctx, 0, p, false, false); err != nil {
		t.Fatal(err)
	}
	for _, v := range p {
		if v != 0 {
			t.Fatal("cached data not zero")
		}
	}
	p[511] = 1
	if err := s.Put(ctx, 0, p); err != nil {
		t.Fatal(err)
	}
	if strings.Join(methods, ",") != "DELETE,DELETE,PUT" {
		t.Fatal(methods)
	}
}
