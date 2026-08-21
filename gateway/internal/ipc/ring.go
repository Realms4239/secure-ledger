package ipc

import (
	"encoding/binary"
	"errors"
	"os"
	"sync"
	"sync/atomic"
)

// ponytail: file-backed fallback for Windows dev; mmap on WSL2/Linux via syscall.Mmap
// is the preferred bench path. Upgrade: add ring_unix.go with //go:build !windows
// using syscall.Mmap when WSL2 integration runs.

const headerSize = 256

// Offsets per proto/ipc-wire.md §1
const (
	offHead     = 0   // AtomicU64
	offTail     = 64  // AtomicU64 (padded)
	offCapacity = 128 // u64 LE
	offMagic    = 136 // u32 LE
	offVersion  = 140 // u16 LE
)

var ErrRingFull = errors.New("ring full")

type RingWriter struct {
	file       *os.File
	capacity   uint64
	mask       uint64
	headerSize int64
	head       atomic.Uint64
	tail       atomic.Uint64
	mu         sync.Mutex
	closed     atomic.Bool
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func NewRingWriter(path string, numSlots int) (*RingWriter, error) {
	if numSlots <= 0 {
		return nil, errors.New("ipc: numSlots must be >0")
	}
	if !isPowerOfTwo(numSlots) {
		return nil, errors.New("ipc: capacity must be power-of-two")
	}
	hs := int64(headerSize)
	fileSize := hs + int64(numSlots)*int64(SlotSize)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	// Ensure size
	if err := f.Truncate(fileSize); err != nil {
		f.Close()
		return nil, err
	}
	// Initialize or validate header
	// Read existing header to check if already initialized
	hdr := make([]byte, headerSize)
	if _, err := f.ReadAt(hdr, 0); err != nil {
		// if read fails, treat as empty
	}
	magic := binary.LittleEndian.Uint32(hdr[offMagic : offMagic+4])
	ver := binary.LittleEndian.Uint16(hdr[offVersion : offVersion+2])
	capacityStored := binary.LittleEndian.Uint64(hdr[offCapacity : offCapacity+8])
	headStored := binary.LittleEndian.Uint64(hdr[offHead : offHead+8])
	tailStored := binary.LittleEndian.Uint64(hdr[offTail : offTail+8])

	needInit := false
	if magic == 0 && ver == 0 && capacityStored == 0 {
		needInit = true
	} else {
		// validate existing header
		if magic != Magic {
			f.Close()
			return nil, errors.New("ipc: magic mismatch")
		}
		if ver != Version {
			f.Close()
			return nil, errors.New("ipc: version mismatch")
		}
		if capacityStored != uint64(numSlots) {
			f.Close()
			return nil, errors.New("ipc: capacity mismatch")
		}
	}
	rw := &RingWriter{
		file:       f,
		capacity:   uint64(numSlots),
		mask:       uint64(numSlots - 1),
		headerSize: hs,
	}
	if needInit {
		// zero header (truncate already zeroed but ensure)
		// write capacity, magic, version, head/tail zero
		var h [headerSize]byte
		binary.LittleEndian.PutUint64(h[offHead:], 0)
		binary.LittleEndian.PutUint64(h[offTail:], 0)
		binary.LittleEndian.PutUint64(h[offCapacity:], uint64(numSlots))
		binary.LittleEndian.PutUint32(h[offMagic:], Magic)
		binary.LittleEndian.PutUint16(h[offVersion:], Version)
		binary.LittleEndian.PutUint16(h[offVersion+2:], 0) // reserved
		if _, err := f.WriteAt(h[:], 0); err != nil {
			f.Close()
			return nil, err
		}
		rw.head.Store(0)
		rw.tail.Store(0)
	} else {
		rw.head.Store(headStored)
		rw.tail.Store(tailStored)
	}
	return rw, nil
}

func (r *RingWriter) Write(typ byte, sagaID [16]byte, payload []byte) error {
	if r.closed.Load() {
		return errors.New("ipc: ring closed")
	}
	// Honest backpressure: check head-tail >= capacity
	// For file fallback, tail is only updated by external reader via file.
	// Try to refresh tail from file header to see reader progress.
	r.mu.Lock()
	defer r.mu.Unlock()

	// refresh tail from shared header (best-effort)
	var tailBytes [8]byte
	if _, err := r.file.ReadAt(tailBytes[:], offTail); err == nil {
		tailFromFile := binary.LittleEndian.Uint64(tailBytes[:])
		// only move forward (reader monotonic)
		if tailFromFile > r.tail.Load() {
			r.tail.Store(tailFromFile)
		}
	}
	head := r.head.Load()
	tail := r.tail.Load()
	if head-tail >= r.capacity {
		return ErrRingFull
	}
	// also check payload size early to return marshal error before claiming slot
	if len(payload) > PayloadMax {
		return errors.New("ipc: payload too large")
	}
	buf := make([]byte, SlotSize)
	if err := MarshalSlot(buf, typ, sagaID, payload); err != nil {
		return err
	}
	idx := head & r.mask
	offset := r.headerSize + int64(idx)*int64(SlotSize)
	if _, err := r.file.WriteAt(buf, offset); err != nil {
		return err
	}
	newHead := head + 1
	r.head.Store(newHead)
	var hb [8]byte
	binary.LittleEndian.PutUint64(hb[:], newHead)
	if _, err := r.file.WriteAt(hb[:], offHead); err != nil {
		return err
	}
	return nil
}

// ReadRaw returns a copy of the 256 B slot at logical index idx (idx & mask).
// Helper for tests; not part of SPSC protocol.
func (r *RingWriter) ReadRaw(idx uint64) ([]byte, error) {
	if r.closed.Load() {
		return nil, errors.New("ipc: ring closed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	actual := idx & r.mask
	offset := r.headerSize + int64(actual)*int64(SlotSize)
	buf := make([]byte, SlotSize)
	if _, err := r.file.ReadAt(buf, offset); err != nil {
		return nil, err
	}
	return buf, nil
}

func (r *RingWriter) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	return r.file.Close()
}
