package main

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
)

// Unix sockets live in a private directory so they are protected even between
// bind and chmod. Never unlink an existing path: another server may own it.
func listenNBD(address string) (net.Listener, error) {
	path, unix := strings.CutPrefix(address, "unix:")
	if !unix {
		return net.Listen("tcp", address)
	}
	if !filepath.IsAbs(path) {
		return nil, fmt.Errorf("Unix socket path must be absolute")
	}
	dir := filepath.Dir(path)
	info, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("Unix socket directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm() != 0700 {
		return nil, fmt.Errorf("Unix socket directory %s must have mode 0700", dir)
	}
	l, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		l.Close()
		return nil, fmt.Errorf("Unix socket permissions: %w", err)
	}
	return l, nil
}
