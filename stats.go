package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type methodStats struct{ Requests, Sent, Received uint64 }
type statsTransport struct {
	base   http.RoundTripper
	mu     sync.Mutex
	counts map[string]methodStats
}

func (s *statsTransport) add(method string, requests, sent, received uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.counts == nil {
		s.counts = map[string]methodStats{}
	}
	c := s.counts[method]
	c.Requests += requests
	c.Sent += sent
	c.Received += received
	s.counts[method] = c
}

type countedBody struct {
	io.ReadCloser
	count func(uint64)
}

func (b *countedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	b.count(uint64(n))
	return n, err
}
func (s *statsTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	method := req.Method
	if method == http.MethodPost && req.URL.Query().Has("delete") {
		method = "POST(DeleteObjects)"
	}
	s.add(method, 1, 0, 0)
	r := req.Clone(req.Context())
	if req.Body != nil {
		r.Body = &countedBody{req.Body, func(n uint64) { s.add(method, 0, n, 0) }}
	}
	resp, err := s.base.RoundTrip(r)
	if resp != nil && resp.Body != nil {
		resp.Body = &countedBody{resp.Body, func(n uint64) { s.add(method, 0, 0, n) }}
	}
	return resp, err
}
func (s *statsTransport) snapshot() map[string]methodStats {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.counts
	s.counts = map[string]methodStats{}
	return out
}
func (s *statsTransport) logLoop(ctx context.Context, devices map[string]*Device) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	names := make([]string, 0, len(devices))
	for name := range devices {
		names = append(names, name)
	}
	sort.Strings(names)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			counts := s.snapshot()
			methods := make([]string, 0, len(counts))
			for method := range counts {
				methods = append(methods, method)
			}
			sort.Strings(methods)
			parts := make([]string, 0, len(methods))
			for _, method := range methods {
				c := counts[method]
				parts = append(parts, fmt.Sprintf("%s requests=%d tx_bytes=%d rx_bytes=%d", method, c.Requests, c.Sent, c.Received))
			}
			if len(parts) == 0 {
				parts = append(parts, "requests=0 tx_bytes=0 rx_bytes=0")
			}
			log.Printf("S3 stats interval=5s: %s", strings.Join(parts, "; "))
			for _, name := range names {
				d := devices[name]
				dirty := d.dirtyBlocks.Load()
				log.Printf("cache stats device=%q dirty_objects=%d dirty_bytes=%d", name, dirty, dirty*int64(d.blockSize))
			}
		}
	}
}
