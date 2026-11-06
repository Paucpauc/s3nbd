package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/minio/minio-go/v7/pkg/signer"
)

var errObjectMissing = errors.New("S3 object does not exist")

// Yandex PATCH is an opt-in extension, not part of the standard S3 API.
type yandexStore struct {
	*s3Store
	httpClient                          *http.Client
	region, accessKey, secretKey, token string
}

func (s *yandexStore) Patch(ctx context.Context, id uint64, offset int, p []byte) error {
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	u := *s.client.EndpointURL()
	u.Path = "/" + s.bucket + "/" + s.key(id)
	u.RawPath = ""
	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, u.String(), bytes.NewReader(p))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/*", offset, offset+len(p)-1))
	req.Header.Set("Content-Type", "application/octet-stream")
	sum := sha256.Sum256(p)
	req.Header.Set("X-Amz-Content-Sha256", fmt.Sprintf("%x", sum))
	req = signer.SignV4(*req, s.accessKey, s.secretKey, s.token, s.region)
	resp, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		var detail struct{ Code string }
		_ = xml.Unmarshal(body, &detail)
		if resp.StatusCode == http.StatusNotFound && detail.Code == "NoSuchKey" {
			return fmt.Errorf("PATCH block %d: %w", id, errObjectMissing)
		}
		return fmt.Errorf("PATCH block %d: HTTP %d: %s", id, resp.StatusCode, body)
	}
	return nil
}
