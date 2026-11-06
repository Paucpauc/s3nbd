package main

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Config struct {
	Listen, Endpoint, Region                      string
	LogLevel                                      string
	InsecureTLS                                   bool
	YandexPatch                                   bool
	Workers, BlockSize, CacheBlocks, FlushSeconds int
	Devices                                       []Export
}
type Export struct {
	Name, Bucket, Prefix string
	Size                 uint64
}

func run() error {
	path := flag.String("config", "config.json", "configuration file")
	flag.Parse()
	raw, err := os.ReadFile(*path)
	if err != nil {
		return err
	}
	cfg := Config{Listen: "127.0.0.1:10809", Region: "us-east-1", Workers: 16, BlockSize: 1 << 20, CacheBlocks: 256, FlushSeconds: 5}
	if err = json.Unmarshal(raw, &cfg); err != nil {
		return err
	}
	logger, err := newLogger(os.Stderr, cfg.LogLevel)
	if err != nil {
		return err
	}
	slog.SetDefault(logger)
	if cfg.Workers < 1 || cfg.BlockSize < 512 || cfg.BlockSize > 32<<20 || cfg.BlockSize%512 != 0 || cfg.CacheBlocks < 1 || cfg.FlushSeconds < 1 {
		return fmt.Errorf("invalid cache/worker settings")
	}
	u, err := url.Parse(cfg.Endpoint)
	if err != nil {
		return err
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.Path != "" && u.Path != "/" {
		return fmt.Errorf("endpoint must be http(s)://host[:port]")
	}
	if cfg.InsecureTLS {
		slog.Warn("S3 TLS certificate verification disabled")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureTLS}
	transport.MaxIdleConnsPerHost = cfg.Workers
	transport.ResponseHeaderTimeout = 60 * time.Second
	stats := &statsTransport{base: transport}
	client, err := minio.New(u.Host, &minio.Options{Creds: credentials.NewStaticV4(os.Getenv("S3_ACCESS_KEY"), os.Getenv("S3_SECRET_KEY"), os.Getenv("S3_SESSION_TOKEN")), Secure: u.Scheme == "https", Region: cfg.Region, Transport: stats, BucketLookup: minio.BucketLookupPath})
	if err != nil {
		return err
	}
	devices := map[string]*Device{}
	locations := map[string]bool{}
	for _, e := range cfg.Devices {
		e.Prefix = strings.Trim(e.Prefix, "/")
		key := e.Bucket + "/" + e.Prefix
		if e.Name == "" || e.Bucket == "" || e.Prefix == "" || e.Size == 0 || e.Size%uint64(cfg.BlockSize) != 0 || devices[e.Name] != nil || locations[key] {
			return fmt.Errorf("invalid or duplicate export %q", e.Name)
		}
		locations[key] = true
		base := &s3Store{client, e.Bucket, e.Prefix, cfg.BlockSize}
		var store Store = base
		if cfg.YandexPatch {
			store = &yandexStore{s3Store: base, httpClient: &http.Client{Transport: stats, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, region: cfg.Region, accessKey: os.Getenv("S3_ACCESS_KEY"), secretKey: os.Getenv("S3_SECRET_KEY"), token: os.Getenv("S3_SESSION_TOKEN")}
		}
		devices[e.Name] = &Device{store: store, size: e.Size, blockSize: cfg.BlockSize, capacity: cfg.CacheBlocks, workers: cfg.Workers, blocks: map[uint64]*block{}}
	}
	if len(devices) == 0 {
		return fmt.Errorf("no devices")
	}
	listener, err := listenNBD(cfg.Listen)
	if err != nil {
		return err
	}
	defer listener.Close()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	var cm sync.Mutex
	connections := map[net.Conn]bool{}
	go func() {
		<-ctx.Done()
		listener.Close()
		cm.Lock()
		for c := range connections {
			c.Close()
		}
		cm.Unlock()
	}()
	var bg sync.WaitGroup
	bg.Add(1)
	go func() { defer bg.Done(); stats.logLoop(ctx, devices) }()
	bg.Add(1)
	go func() {
		defer bg.Done()
		t := time.NewTicker(time.Duration(cfg.FlushSeconds) * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				for name, d := range devices {
					if err := d.Flush(context.Background()); err != nil {
						slog.Error("background flush failed", "device", name, "error", err)
					}
				}
			}
		}
	}()
	slog.Info("NBD listening", "address", cfg.Listen)
	for {
		c, err := listener.Accept()
		if err != nil {
			if ctx.Err() == nil {
				stop()
			}
			break
		}
		cm.Lock()
		if ctx.Err() != nil {
			cm.Unlock()
			c.Close()
			break
		}
		connections[c] = true
		cm.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { cm.Lock(); delete(connections, c); cm.Unlock() }()
			serve(c, devices)
		}()
	}
	slog.Info("NBD shutting down")
	wg.Wait()
	bg.Wait()
	return flushDevices(context.Background(), devices)
}

// A failed export must not prevent the remaining devices from being saved.
func flushDevices(ctx context.Context, devices map[string]*Device) error {
	var errs []error
	for name, d := range devices {
		if err := d.Flush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("final flush %s: %w", name, err))
		}
	}
	return errors.Join(errs...)
}
func main() {
	if err := run(); err != nil {
		slog.Error("server failed", "error", err)
		os.Exit(1)
	}
}
