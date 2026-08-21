package ipc

import (
	"bytes"
	"testing"
)

func TestMarshalUnmarshalSlot_RoundTrip(t *testing.T) {
	var id [16]byte
	copy(id[:], []byte("test-saga-id-123"))
	payload := []byte(`{"from":"alice","to":"bob","amount":100}`)
	buf := make([]byte, SlotSize)
	if err := MarshalSlot(buf, 0x01, id, payload); err != nil {
		t.Fatal(err)
	}
	typ, gotID, gotPayload, err := UnmarshalSlot(buf)
	if err != nil {
		t.Fatal(err)
	}
	if typ != 0x01 || gotID != id || !bytes.Equal(gotPayload, payload) {
		t.Fatalf("mismatch typ=%x id=%x payload=%q", typ, gotID, gotPayload)
	}
}

func TestMarshalSlot_PayloadTooLarge(t *testing.T) {
	var id [16]byte
	buf := make([]byte, SlotSize)
	large := bytes.Repeat([]byte("x"), PayloadMax+1)
	if err := MarshalSlot(buf, 0x01, id, large); err == nil {
		t.Fatal("want err for payload >224B")
	}
}
