package ipc

import (
	"os"
	"testing"
)

func TestRingWriter_WriteAndReadRaw(t *testing.T) {
	path := os.TempDir() + "/test-secureledger.ring"
	os.Remove(path)
	rw, err := NewRingWriter(path, 1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { rw.Close(); os.Remove(path) }()
	var id [16]byte
	copy(id[:], []byte("ring-test-id-123"))
	if err := rw.Write(0x01, id, []byte(`{"x":1}`)); err != nil {
		t.Fatal(err)
	}
	slot, err := rw.ReadRaw(0)
	if err != nil {
		t.Fatal(err)
	}
	typ, gotID, payload, err := UnmarshalSlot(slot)
	if err != nil || typ != 0x01 || gotID != id {
		t.Fatalf("raw read mismatch %v typ=%x id=%x", err, typ, gotID)
	}
	_ = payload
}

func TestRingWriter_FullReturns503(t *testing.T) {
	path := os.TempDir() + "/test-full.ring"
	os.Remove(path)
	rw, err := NewRingWriter(path, 4) // tiny ring
	if err != nil {
		t.Fatal(err)
	}
	defer func() { rw.Close(); os.Remove(path) }()
	var id [16]byte
	for i := 0; i < 4; i++ {
		_ = rw.Write(0x01, id, []byte("{}"))
	}
	if err := rw.Write(0x01, id, []byte("{}")); err == nil {
		t.Fatal("want ErrRingFull on 5th write to 4-slot ring without consumer")
	}
}
