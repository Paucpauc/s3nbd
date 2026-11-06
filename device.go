package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

type Store interface {
	Get(context.Context, uint64) ([]byte, error)
	Put(context.Context, uint64, []byte) error
	Delete(context.Context, uint64) error
}
type span struct{ lo, hi int }
type block struct {
	mu             sync.Mutex
	refs           int
	data           []byte
	dirty, deleted bool
	known, changes []span
}
type patchStore interface {
	Patch(context.Context, uint64, int, []byte) error
}

func merge(rs []span, r span) []span {
	out := make([]span, 0, len(rs)+1)
	for _, x := range rs {
		if x.hi < r.lo {
			out = append(out, x)
		} else if r.hi < x.lo {
			out = append(out, r)
			r = x
		} else {
			r.lo = min(r.lo, x.lo)
			r.hi = max(r.hi, x.hi)
		}
	}
	return append(out, r)
}
func covers(rs []span, lo, hi int) bool {
	for _, r := range rs {
		if r.lo <= lo && r.hi >= hi {
			return true
		}
	}
	return false
}
func (b *block) markDirty(lo, hi int) {
	b.known = merge(b.known, span{lo, hi})
	b.changes = merge(b.changes, span{lo, hi})
	b.dirty = true
}

// Load only unknown bytes, retaining buffered writes and successfully patched ranges.
func (d *Device) load(ctx context.Context, id uint64, b *block) error {
	if covers(b.known, 0, len(b.data)) {
		return nil
	}
	p, err := d.store.Get(ctx, id)
	if err != nil {
		return err
	}
	if len(p) != 0 && len(p) != len(b.data) {
		return fmt.Errorf("invalid block %d length", id)
	}
	if len(p) == 0 {
		p = make([]byte, len(b.data))
	}
	for _, r := range b.known {
		copy(p[r.lo:r.hi], b.data[r.lo:r.hi])
	}
	copy(b.data, p)
	b.known = []span{{0, len(b.data)}}
	return nil
}
func (d *Device) save(ctx context.Context, id uint64, b *block) error {
	if b.deleted {
		return d.store.Delete(ctx, id)
	}
	if ps, ok := d.store.(patchStore); ok && !covers(b.changes, 0, len(b.data)) {

		for _, r := range b.changes {
			err := ps.Patch(ctx, id, r.lo, b.data[r.lo:r.hi])
			if errors.Is(err, errObjectMissing) {
				// No object to preserve: unknown bytes in the lazy buffer are zero.
				b.known = []span{{0, len(b.data)}}
				return d.store.Put(ctx, id, b.data)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}

	if err := d.load(ctx, id, b); err != nil {
		return err
	}
	return d.store.Put(ctx, id, b.data)
}

// gate allows concurrent reads and makes mutations/flush exclusive.
// mu protects cache membership and references; block.mu protects loading.
type Device struct {
	dirtyBlocks                  atomic.Int64
	gate                         sync.RWMutex
	mu                           sync.Mutex
	changed                      *sync.Cond
	slots                        chan struct{}
	store                        Store
	size                         uint64
	blockSize, capacity, workers int
	blocks                       map[uint64]*block
	busy                         bool
}

func (d *Device) valid(off uint64, n int) bool { return off <= d.size && uint64(n) <= d.size-off }

// DeleteBatch returns one result per ID; a missing result is never success.
type batchDeleter interface {
	DeleteBatch(context.Context, []uint64) map[uint64]error
}

func (d *Device) flush(ctx context.Context) error {
	type job struct {
		ids    []uint64
		remove bool
	}
	var pending []job
	var deletes []uint64
	bulk, canBatch := d.store.(batchDeleter)
	for id, b := range d.blocks {
		if !b.dirty {
			continue
		}
		if b.deleted && canBatch {
			deletes = append(deletes, id)
		} else {
			pending = append(pending, job{ids: []uint64{id}})
		}
	}
	for len(deletes) > 0 {
		n := min(1000, len(deletes))
		pending = append(pending, job{ids: deletes[:n], remove: true})
		deletes = deletes[n:]
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	var em sync.Mutex
	var first error
	for i := 0; i < min(d.workers, len(pending)); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				var results map[uint64]error
				if j.remove {
					results = bulk.DeleteBatch(ctx, j.ids)
				}
				for _, id := range j.ids {
					b := d.blocks[id]
					b.mu.Lock()
					var err error
					if j.remove {
						var ok bool
						err, ok = results[id]
						if !ok {
							err = fmt.Errorf("missing delete result for block %d", id)
						}
					} else {
						err = d.save(ctx, id, b)
					}
					if err != nil {
						em.Lock()
						if first == nil {
							first = err
						}
						em.Unlock()
					} else {
						d.dirtyBlocks.Add(-1)
						b.dirty = false
						b.changes = nil
					}
					b.mu.Unlock()
				}
			}
		}()
	}
	for _, j := range pending {
		jobs <- j
	}
	close(jobs)
	wg.Wait()
	return first
}
func (d *Device) Flush(ctx context.Context) error {
	d.gate.Lock()
	defer d.gate.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.flush(ctx)
}
func (d *Device) get(ctx context.Context, id uint64) (*block, error) {
	if b := d.blocks[id]; b != nil {
		return b, nil
	}
	for len(d.blocks) >= d.capacity {
		var victim uint64
		found := false
		for k, b := range d.blocks {
			if b.refs == 0 && !b.dirty {
				victim = k
				found = true
				break
			}
		}
		if !found {
			unpinned := false
			for _, b := range d.blocks {
				if b.refs == 0 {
					unpinned = true
					break
				}
			}
			if !unpinned {
				d.changed.Wait()
				continue
			}
			if err := d.flush(ctx); err != nil {
				return nil, err
			}
			for k, b := range d.blocks {
				if b.refs != 0 {
					continue
				}
				victim = k
				break
			}
		}
		delete(d.blocks, victim)
	}
	b := &block{data: make([]byte, d.blockSize)}
	d.blocks[id] = b
	return b, nil
}
func (d *Device) IO(ctx context.Context, off uint64, p []byte, write, fua bool) error {
	if !write {
		d.gate.RLock()
		defer d.gate.RUnlock()
		if !d.valid(off, len(p)) {
			return fmt.Errorf("out of bounds")
		}
		return d.read(ctx, off, p)
	}
	d.gate.Lock()
	defer d.gate.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.valid(off, len(p)) {
		return fmt.Errorf("out of bounds")
	}
	if !write {
		return d.read(ctx, off, p)
	}
	for len(p) > 0 {
		id := off / uint64(d.blockSize)
		start := int(off % uint64(d.blockSize))
		n := min(len(p), d.blockSize-start)
		b, err := d.get(ctx, id)
		if err != nil {
			return err
		}
		copy(b.data[start:start+n], p[:n])
		if !b.dirty {
			d.dirtyBlocks.Add(1)
		}
		b.markDirty(start, start+n)
		b.deleted = false
		p = p[n:]
		off += uint64(n)
	}
	if fua {
		return d.flush(ctx)
	}
	return nil
}

// Discard is buffered like WRITE; FLUSH/FUA make deletions durable.
func (d *Device) Discard(ctx context.Context, off uint64, length int, fua bool) error {
	d.gate.Lock()
	defer d.gate.Unlock()
	d.mu.Lock()
	defer d.mu.Unlock()
	if !d.valid(off, length) {
		return fmt.Errorf("out of bounds")
	}
	for length > 0 {
		id := off / uint64(d.blockSize)
		start := int(off % uint64(d.blockSize))
		n := min(length, d.blockSize-start)
		full := start == 0 && n == d.blockSize
		b, err := d.get(ctx, id)
		if err != nil {
			return err
		}
		clear(b.data[start : start+n])
		if !b.dirty {
			d.dirtyBlocks.Add(1)
		}
		b.markDirty(start, start+n)
		if full {
			b.deleted = true
		}
		off += uint64(n)
		length -= n
	}
	if fua {
		return d.flush(ctx)
	}
	return nil
}

func (d *Device) readBlock(ctx context.Context, id uint64, start int, out []byte) error {
	d.mu.Lock()
	if d.slots == nil {
		d.slots = make(chan struct{}, d.workers)
		d.changed = sync.NewCond(&d.mu)
	}
	slots := d.slots
	d.mu.Unlock()
	select {
	case slots <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	defer func() { <-slots }()
	d.mu.Lock()
	b, err := d.get(ctx, id)
	if err != nil {
		d.mu.Unlock()
		return err
	}
	b.refs++
	d.mu.Unlock()
	defer func() { d.mu.Lock(); b.refs--; d.changed.Broadcast(); d.mu.Unlock() }()
	b.mu.Lock()
	defer b.mu.Unlock()
	if !covers(b.known, start, start+len(out)) {
		if err := d.load(ctx, id, b); err != nil {
			return err
		}
	}
	copy(out, b.data[start:start+len(out)])
	return nil
}
func (d *Device) read(ctx context.Context, off uint64, p []byte) error {
	for len(p) > 0 {
		var wg sync.WaitGroup
		errs := make([]error, 0, d.workers)
		type job struct {
			id    uint64
			start int
			out   []byte
		}
		jobs := make([]job, 0, d.workers)
		for len(p) > 0 && len(jobs) < d.workers {
			start := int(off % uint64(d.blockSize))
			n := min(len(p), d.blockSize-start)
			jobs = append(jobs, job{off / uint64(d.blockSize), start, p[:n]})
			p = p[n:]
			off += uint64(n)
			errs = append(errs, nil)
		}
		for i, j := range jobs {
			wg.Add(1)
			go func() { defer wg.Done(); errs[i] = d.readBlock(ctx, j.id, j.start, j.out) }()
		}
		wg.Wait()
		for _, err := range errs {
			if err != nil {
				return err
			}
		}
	}
	return nil
}
