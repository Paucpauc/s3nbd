package main

import (
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUnixListenerNBD(t *testing.T) {
	// Short paths also fit macOS's sockaddr_un limit.
	dir, err := os.MkdirTemp("/tmp", "s3nbd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "nbd.sock")
	l, err := listenNBD("unix:" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0600 {
		t.Fatalf("socket permissions: %v", info.Mode())
	}
	if other, err := listenNBD("unix:" + path); err == nil {
		other.Close()
		t.Fatal("replaced active listener")
	}
	d := &Device{store: &memoryStore{data: map[uint64][]byte{}}, size: 512, blockSize: 512, capacity: 1, workers: 1, blocks: map[uint64]*block{}}
	done := make(chan error, 1)
	go func() {
		c, err := l.Accept()
		if err == nil {
			serve(c, map[string]*Device{"disk": d})
		}
		done <- err
	}()
	c, err := net.DialTimeout("unix", path, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	if _, err := io.ReadFull(c, make([]byte, 18)); err != nil {
		t.Fatal(err)
	}
	if err := send(c, uint32(3), optMagic, uint32(1), uint32(4), []byte("disk")); err != nil {
		t.Fatal(err)
	}
	h := make([]byte, 10)
	if _, err := io.ReadFull(c, h); err != nil {
		t.Fatal(err)
	}
	if be.Uint64(h) != d.size {
		t.Fatal("wrong export size")
	}
	if err := send(c, uint32(0x25609513), uint16(0), uint16(2), uint64(0), uint64(0), uint32(0)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not disconnect")
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatalf("socket not removed on close: %v", err)
	}
}

func TestUnixListenerRejectsUnsafePaths(t *testing.T) {
	if l, err := listenNBD("unix:relative.sock"); err == nil {
		l.Close()
		t.Fatal("accepted relative socket path")
	}
	dir, err := os.MkdirTemp("/tmp", "s3nbd-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "nbd.sock")
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if l, err := listenNBD("unix:" + path); err == nil {
		l.Close()
		t.Fatal("replaced existing file")
	}
	if p, err := os.ReadFile(path); err != nil || string(p) != "keep" {
		t.Fatalf("existing file changed: %q, %v", p, err)
	}
	if err := os.Chmod(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if l, err := listenNBD("unix:" + filepath.Join(dir, "other.sock")); err == nil {
		l.Close()
		t.Fatal("accepted non-private directory")
	}
}

func TestTCPListener(t *testing.T) {
	l, err := listenNBD("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	c, err := net.DialTimeout("tcp", l.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
}
