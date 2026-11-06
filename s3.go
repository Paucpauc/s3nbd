package main

import (
	"bytes"
	"context"
	"fmt"
	"github.com/minio/minio-go/v7"
	"io"
	"time"
)

type s3Store struct {
	client         *minio.Client
	bucket, prefix string
	blockSize      int
}

func (s *s3Store) key(id uint64) string { return fmt.Sprintf("%s/blocks/%016x", s.prefix, id) }
func (s *s3Store) Get(ctx context.Context, id uint64) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	o, err := s.client.GetObject(ctx, s.bucket, s.key(id), minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer o.Close()
	p, err := io.ReadAll(io.LimitReader(o, int64(s.blockSize)+1))
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, nil
	}
	return p, err
}
func (s *s3Store) Put(ctx context.Context, id uint64, p []byte) error {
	// Missing objects read as zeros. Only inspect a complete PUT payload,
	// never a partial PATCH whose untouched bytes may contain data.
	zero := true
	for _, v := range p {
		if v != 0 {
			zero = false
			break
		}
	}
	if zero {
		return s.Delete(ctx, id)
	}
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	_, err := s.client.PutObject(ctx, s.bucket, s.key(id), bytes.NewReader(p), int64(len(p)), minio.PutObjectOptions{ContentType: "application/octet-stream", DisableMultipart: true})
	return err
}

func (s *s3Store) Delete(ctx context.Context, id uint64) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	return s.client.RemoveObject(ctx, s.bucket, s.key(id), minio.RemoveObjectOptions{})
}

// Require explicit success for each key, including when HTTP status is 200.
func (s *s3Store) DeleteBatch(ctx context.Context, ids []uint64) map[uint64]error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	results := make(map[uint64]error, len(ids))
	keys := make(map[string]uint64, len(ids))
	objects := make(chan minio.ObjectInfo, len(ids))
	for _, id := range ids {
		key := s.key(id)
		keys[key] = id
		results[id] = fmt.Errorf("missing delete result for block %d", id)
		objects <- minio.ObjectInfo{Key: key}
	}
	close(objects)
	for result := range s.client.RemoveObjectsWithResult(ctx, s.bucket, objects, minio.RemoveObjectsOptions{}) {
		if id, ok := keys[result.ObjectName]; ok {
			results[id] = result.Err
		} else if result.Err != nil {
			for id := range results {
				results[id] = result.Err
			}
		}
	}
	return results
}
