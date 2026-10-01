package ipc

import (
	"encoding/binary"
	"errors"
	"os"
	"reflect"
	"sync"
	"sync/atomic"
	"unsafe"

	"golang.org/x/sys/windows"
)

// sliceFromPtr wraps a MapViewOfFile result without a uintptr→unsafe.Pointer
// conversion (keeps go vet unsafeptr clean). ponytail: reflect.SliceHeader is
// deprecated; revisit if it's ever removed.
func sliceFromPtr(addr uintptr, size uint64) []byte {
	var b []byte
	sh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	sh.Data = addr
	sh.Len = int(size)
	sh.Cap = int(size)
	return b
}

// Both sides map the same file: CreateFileMapping sections are shared across
// processes, so slot data + counters written here are visible to the Rust
// reader's memmap2 view without flushes. (File-I/O vs mmap is NOT coherent
// on Windows — this is why the writer must mmap too.)

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
	section    windows.Handle
	addr       uintptr
	file       *os.File
	mmap       []byte
	capacity   uint64
	mask       uint64
	headerSize int64
	head       uint64 // local cached counter, authoritative value lives in mmap
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
	fileSize := uint64(hs) + uint64(numSlots)*uint64(SlotSize)

	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(fileSize)); err != nil {
		f.Close()
		return nil, err
	}
	// PAGE_READWRITE, SEC_RESERVE default; map whole file shared.
	h, err := windows.CreateFileMapping(windows.Handle(f.Fd()), nil, windows.PAGE_READWRITE, uint32(fileSize>>32), uint32(fileSize), nil)
	if err != nil && err != windows.ERROR_ALREADY_EXISTS {
		f.Close()
		return nil, err
	}
	addr, err := windows.MapViewOfFile(h, windows.FILE_MAP_WRITE, 0, 0, uintptr(fileSize))
	if err != nil {
		windows.CloseHandle(h)
		f.Close()
		return nil, err
	}
	mapAll := sliceFromPtr(addr, fileSize)

	rw := &RingWriter{
		section:    h,
		addr:       addr,
		file:       f,
		mmap:       mapAll,
		capacity:   uint64(numSlots),
		mask:       uint64(numSlots - 1),
		headerSize: hs,
	}

	magic := binary.LittleEndian.Uint32(mapAll[offMagic : offMagic+4])
	ver := binary.LittleEndian.Uint16(mapAll[offVersion : offVersion+2])
	capStored := binary.LittleEndian.Uint64(mapAll[offCapacity : offCapacity+8])
	if magic == 0 && ver == 0 && capStored == 0 {
		hdr := make([]byte, headerSize)
		binary.LittleEndian.PutUint64(hdr[offCapacity:], uint64(numSlots))
		binary.LittleEndian.PutUint32(hdr[offMagic:], Magic)
		binary.LittleEndian.PutUint16(hdr[offVersion:], Version)
		copy(mapAll[:headerSize], hdr)
	} else {
		if magic != Magic {
			rw.Close()
			return nil, errors.New("ipc: magic mismatch")
		}
		if ver != Version {
			rw.Close()
			return nil, errors.New("ipc: version mismatch")
		}
		if capStored != uint64(numSlots) {
			rw.Close()
			return nil, errors.New("ipc: capacity mismatch")
		}
	}
	rw.head = binary.LittleEndian.Uint64(mapAll[offHead : offHead+8])
	return rw, nil
}

func (r *RingWriter) loadTail() uint64 {
	return binary.LittleEndian.Uint64(r.mmap[offTail : offTail+8])
}

func (r *RingWriter) storeHead(v uint64) {
	binary.LittleEndian.PutUint64(r.mmap[offHead:offHead+8], v)
	r.head = v
}

func (r *RingWriter) Write(typ byte, sagaID [16]byte, payload []byte) error {
	if r.closed.Load() {
		return errors.New("ipc: ring closed")
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	tail := r.loadTail()
	if r.head-tail >= r.capacity {
		return ErrRingFull
	}
	if len(payload) > PayloadMax {
		return errors.New("ipc: payload too large")
	}
	slot := r.mmap[r.headerSize+(int64(r.head&r.mask))*SlotSize:]
	buf := make([]byte, SlotSize)
	if err := MarshalSlot(buf, typ, sagaID, payload); err != nil {
		return err
	}
	copy(slot, buf) // slot data lands before head publish (Release semantics)
	r.storeHead(r.head + 1)
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
	out := make([]byte, SlotSize)
	copy(out, r.mmap[offset:offset+SlotSize])
	return out, nil
}

func (r *RingWriter) Close() error {
	if r.closed.Swap(true) {
		return nil
	}
	// Unmap before closing the file so Windows allows deletion of the ring.
	var err error
	if r.addr != 0 {
		if uerr := windows.UnmapViewOfFile(r.addr); uerr != nil {
			err = uerr
		}
	}
	if r.section != 0 {
		if cerr := windows.CloseHandle(r.section); cerr != nil && err == nil {
			err = cerr
		}
	}
	r.addr = 0
	r.section = 0
	if cerr := r.file.Close(); cerr != nil && err == nil {
		err = cerr
	}
	return err
}
