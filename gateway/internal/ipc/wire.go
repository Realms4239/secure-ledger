package ipc

import (
	"encoding/binary"
	"errors"
	"time"
)

const (
	SlotSize   = 256
	PayloadMax = 224
	Magic      = 0x534C5257
	Version    = 1
)

// Slot layout per proto/ipc-wire.md §2 (256 B, LE):
// 0     type          u8
// 1     flags         u8 (0)
// 2     reserved      u16 LE (0)
// 4     saga_id       [16]byte
// 20    timestamp_ns  u64 LE
// 28    payload_len   u16 LE
// 30    _pad          u16 LE (0)
// 32    payload       [224]byte zero-padded

// MarshalSlot writes typ, sagaID and payload into slot (must be 256 B).
// It zeroes the slot, sets timestamp to time.Now().UnixNano(), and
// payload_len to len(payload). Returns error if slot length !=256 or
// payload too large.
func MarshalSlot(slot []byte, typ byte, sagaID [16]byte, payload []byte) error {
	if len(slot) != SlotSize {
		return errors.New("ipc: slot must be 256 bytes")
	}
	if len(payload) > PayloadMax {
		return errors.New("ipc: payload too large")
	}
	// zero pad entire slot
	for i := range slot {
		slot[i] = 0
	}
	slot[0] = typ
	slot[1] = 0 // flags
	binary.LittleEndian.PutUint16(slot[2:4], 0)
	copy(slot[4:20], sagaID[:])
	binary.LittleEndian.PutUint64(slot[20:28], uint64(time.Now().UnixNano()))
	binary.LittleEndian.PutUint16(slot[28:30], uint16(len(payload)))
	binary.LittleEndian.PutUint16(slot[30:32], 0)
	copy(slot[32:32+len(payload)], payload)
	// remainder already zeroed
	return nil
}

// UnmarshalSlot parses a 256 B slot and returns typ, sagaID and payload.
// Payload is a copy of the bytes (payload[0:payload_len]).
func UnmarshalSlot(slot []byte) (byte, [16]byte, []byte, error) {
	if len(slot) != SlotSize {
		return 0, [16]byte{}, nil, errors.New("ipc: slot must be 256 bytes")
	}
	typ := slot[0]
	// flags at slot[1] ignored, reserved at slot[2:4] ignored
	var sagaID [16]byte
	copy(sagaID[:], slot[4:20])
	// timestamp at slot[20:28] ignored for now
	payloadLen := binary.LittleEndian.Uint16(slot[28:30])
	if int(payloadLen) > PayloadMax {
		return 0, [16]byte{}, nil, errors.New("ipc: payload_len too large")
	}
	// _pad at slot[30:32] ignored
	if int(payloadLen) > SlotSize-32 {
		return 0, [16]byte{}, nil, errors.New("ipc: payload_len exceeds slot")
	}
	// copy payload out so caller does not retain reference to slot
	payload := make([]byte, payloadLen)
	copy(payload, slot[32:32+payloadLen])
	return typ, sagaID, payload, nil
}
