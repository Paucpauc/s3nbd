package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

var be = binary.BigEndian

const optMagic uint64 = 0x49484156454f5054

func send(w io.Writer, v ...any) error {
	for _, x := range v {
		if p, ok := x.([]byte); ok && len(p) == 0 {
			continue
		}
		if err := binary.Write(w, be, x); err != nil {
			return err
		}
	}
	return nil
}
func reply(c net.Conn, opt, kind uint32, p []byte) error {
	return send(c, uint64(0x3e889045565a9), opt, kind, uint32(len(p)), p)
}
func serve(c net.Conn, devices map[string]*Device) {
	defer c.Close()
	c.SetDeadline(time.Now().Add(30 * time.Second))
	if send(c, uint64(0x4e42444d41474943), optMagic, uint16(3)) != nil {
		return
	}
	var flags uint32
	if binary.Read(c, be, &flags) != nil || flags&1 == 0 || flags & ^uint32(3) != 0 {
		return
	}
	var d *Device
	for d == nil {
		var h struct {
			Magic    uint64
			Opt, Len uint32
		}
		if binary.Read(c, be, &h) != nil || h.Magic != optMagic || h.Len > 8192 {
			return
		}
		p := make([]byte, h.Len)
		if _, err := io.ReadFull(c, p); err != nil {
			return
		}
		switch h.Opt {
		case 2:
			reply(c, h.Opt, 1, nil)
			return
		case 3:
			for name := range devices {
				q := make([]byte, 4+len(name))
				be.PutUint32(q, uint32(len(name)))
				copy(q[4:], name)
				if reply(c, h.Opt, 2, q) != nil {
					return
				}
			}
			reply(c, h.Opt, 1, nil)
		case 1, 7, 6:
			name := string(p)
			if h.Opt != 1 {
				if len(p) < 6 {
					reply(c, h.Opt, 0x80000003, nil)
					continue
				}
				n := int(be.Uint32(p))
				if n > len(p)-6 || len(p) != 4+n+2+2*int(be.Uint16(p[4+n:])) {
					reply(c, h.Opt, 0x80000003, nil)
					continue
				}
				name = string(p[4 : 4+n])
			}
			target := devices[name]
			if target == nil {
				if h.Opt == 1 {
					return
				}
				reply(c, h.Opt, 0x80000006, nil)
				continue
			}
			target.mu.Lock()
			busy := target.busy
			if !busy && h.Opt != 6 {
				target.busy = true
			}
			target.mu.Unlock()
			if busy {
				if h.Opt == 1 {
					return
				}
				reply(c, h.Opt, 0x80000001, nil)
				continue
			}
			if h.Opt != 6 {
				d = target
				defer func() {
					if err := d.Flush(context.Background()); err != nil {
						log.Printf("disconnect flush: %v", err)
					}
					d.mu.Lock()
					d.busy = false
					d.mu.Unlock()
				}()
			}
			if h.Opt == 1 {
				if send(c, target.size, uint16(45)) != nil {
					return
				}
				if flags&2 == 0 {
					if send(c, [124]byte{}) != nil {
						return
					}
				}
			} else {
				q := make([]byte, 12)
				be.PutUint64(q[2:], target.size)
				be.PutUint16(q[10:], 45)
				if reply(c, h.Opt, 3, q) != nil {
					return
				}
				if reply(c, h.Opt, 1, nil) != nil {
					return
				}
			}
		default:
			if reply(c, h.Opt, 0x80000001, nil) != nil {
				return
			}
		}
	}
	c.SetDeadline(time.Time{})
	var reads sync.WaitGroup
	var replies sync.Mutex
	readSlots := make(chan struct{}, d.workers)
	ctx, cancel := context.WithCancel(context.Background())
	defer func() { cancel(); c.Close(); reads.Wait() }()

	for {
		var h struct {
			Magic          uint32
			Flags, Type    uint16
			Handle, Offset uint64
			Length         uint32
		}
		if binary.Read(c, be, &h) != nil || h.Magic != 0x25609513 {
			return
		}
		// TRIM has no payload: its range can exceed the READ/WRITE buffer limit.
		if (h.Type == 0 || h.Type == 1) && h.Length > 32<<20 {
			return
		}
		if h.Type == 2 {
			return
		}

		if h.Type == 0 {
			readSlots <- struct{}{}
			reads.Add(1)
			go func() {
				defer reads.Done()
				defer func() { <-readSlots }()
				var errno uint32
				var p []byte
				if h.Flags != 0 || !d.valid(h.Offset, int(h.Length)) {
					errno = 22
				} else {
					p = make([]byte, h.Length)
					if err := d.IO(ctx, h.Offset, p, false, false); err != nil {
						log.Printf("NBD READ: %v", err)
						errno = 5
					}
				}
				replies.Lock()
				defer replies.Unlock()
				if err := send(c, uint32(0x67446698), errno, h.Handle); err != nil {
					c.Close()
					return
				}
				if errno == 0 {
					if err := send(c, p); err != nil {
						c.Close()
					}
				}
			}()
			continue
		}
		reads.Wait()
		var p []byte
		if h.Type == 0 || h.Type == 1 {
			p = make([]byte, h.Length)
		}
		if h.Type == 1 {
			if _, err := io.ReadFull(c, p); err != nil {
				return
			}
		}
		var err error
		var errno uint32
		if h.Flags & ^uint16(1) != 0 || (h.Flags != 0 && h.Type != 1 && h.Type != 4) {
			errno = 22
		} else {
			switch h.Type {
			case 0, 1:
				if !d.valid(h.Offset, len(p)) {
					errno = 22
				} else {
					err = d.IO(context.Background(), h.Offset, p, h.Type == 1, h.Flags&1 != 0)
				}
			case 4:
				if !d.valid(h.Offset, int(h.Length)) {
					errno = 22
				} else {
					err = d.Discard(context.Background(), h.Offset, int(h.Length), h.Flags&1 != 0)
				}
			case 3:
				if h.Offset != 0 || h.Length != 0 {
					errno = 22
				} else {
					err = d.Flush(context.Background())
				}
			default:
				errno = 22
			}
		}
		if err != nil {
			log.Print(fmt.Errorf("NBD command %d: %w", h.Type, err))
			errno = 5
		}
		if send(c, uint32(0x67446698), errno, h.Handle) != nil {
			return
		}
		if h.Type == 0 && errno == 0 {
			if send(c, p) != nil {
				return
			}
		}
	}
}
