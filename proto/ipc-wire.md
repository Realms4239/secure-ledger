# IPC Wire Contract — Normative

> **Experimental / benchmark path.** The shm ring is the WSL2/Linux co-located benchmark path only. Recommended production transport is Postgres outbox / Kafka / NATS / SFTP adapters. See `docs/limitations.md`.

This document is the normative contract between Go `gateway/internal/ipc` and Rust `engine/src/wire.rs` + `engine/src/ring.rs`. Both implementations must match byte-for-byte. Any change requires bumping `VERSION` and updating the fixture test on both sides.

Ring: fixed 256B slots, SPSC, two padded u64 counters (head at 0 and tail at 64, each padded to 64B), JSON payload ≤224B.

## 1. Ring header (file `/dev/shm/secureledger.ring`)

File layout: `header (256 B = one slot-sized, only first 144 B used) + numSlots * 256 B`.

```
Offset  Size  Field       Description
0       8     head        AtomicU64, writer-only, monotonic counter (Release on store)
8       56    _pad0       zero, pads head to 64 B cache line
64      8     tail        AtomicU64, reader-only, monotonic counter (Acquire on load, Release on store)
72      56    _pad1       zero, pads tail to 64 B cache line
128     8     capacity    u64 LE, must be power-of-two (validated on open)
136     4     magic       u32 LE = 0x534C5257 ("SLRW" — Secure Ledger Ring Wire)
140     2     version     u16 LE = 1
142     2     reserved    u16 LE = 0
144     112   _pad2       zero to 256 B boundary
```

Capacity check: `capacity & (capacity - 1) == 0` else reject. Mask: `mask = capacity - 1`. Slot index: `idx = counter & mask`. Byte offset: `headerSize + idx * 256`. headerSize = 256 is normative; slot base = 256 + idx*256.

Memory ordering: writer does `head.load(Acquire)` to check full, copies slot, then `head.fetch_add(1, Release)`. Reader does `tail.load(Acquire)` vs `head.load(Acquire)` to check empty, copies slot, then `tail.fetch_add(1, Release)`. SPSC only — single writer (gateway), single reader (engine).

Ring size math (normative disclaimer): `1M slots * 256 B = 256 MB` in `/dev/shm`, ~100 seconds headroom @10k RPS (`1_000_000 / 10_000 = 100s`). For reference, 24h @10k RPS = 864M events approx 221 GB. The ring is a short backpressure buffer / benchmark path, not durable storage. Ring-full must surface as `503 Service Unavailable` (honest backpressure), not silent drop.

All integers little-endian (LE).

## 2. Slot layout (256 B, fixed)

```
Offset  Size  Field         Type       Description
0       1     type          u8         SAGA_START=0x01, STEP_OK=0x02, STEP_FAIL=0x03, COMPENSATE=0x04
1       1     flags         u8         reserved, must be 0
2       2     reserved      u16 LE     must be 0
4       16    saga_id       [u8;16]    UUID v4 raw bytes (RFC 4122, network order as stored by uuid crate / google/uuid)
20      8     timestamp_ns  u64 LE     CLOCK_REALTIME ns since unix epoch
28      2     payload_len   u16 LE     0..224 inclusive
30      2     _pad          u16 LE     0 (align payload to 32 B boundary)
32      224   payload       [u8;224]   JSON bytes, zero-padded beyond payload_len
```

Invariants: `payload_len <= 224` else writer returns error, reader counts `corrupt_slots_total` and skips. Bytes `payload[payload_len..224]` must be zero (writer zeroes slot before copy). `type` unknown → reader skips + counts corrupt. Truncated slot (file too short) → skip.

JSON payload in slice #1 is `{"from":"alice","to":"bob","amount":100,"idempotency_key":"..."}` truncated to 224 B; larger events require continuation slots (deferred — see `ponytail:` ceiling in spec).

```
Slot diagram (256 B):

 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  type | flags |    reserved   |         saga_id (16 B)        |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    saga_id (cont)                             |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    timestamp_ns (8 B LE)                      |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| payload_len |  _pad  |     payload (224 B, zero-padded)       |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                    payload (cont)                             |
~                        ... 224 B ...                           ~
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

## 3. Fixture (normative cross-language test vector)

Slot with:

- type=0x01 SAGA_START
- saga_id=550e8400-e29b-41d4-a716-446655440000
- timestamp_ns=1700000000000000000
- payload_len=0x0017 (23)
- payload=`{"from":"alice","to":"bob","amount":100}` (23 B)

```
Fixture slot (hex, 32B header prefix):
type=0x01 SAGA_START, saga_id=550e8400-e29b-41d4-a716-446655440000,
timestamp_ns=1700000000000000000, payload_len=0x0017,
payload={"from":"alice","to":"bob","amount":100}
Header hex: 01 00 00 00 55 0e 84 00 e2 9b 41 d4 a7 16 44 66 55 44 00 00 00 1d 1f 8b 7a 9d 8c 00 00 17 00
Payload hex (23 B): 7b 22 66 72 6f 6d 22 3a 22 61 6c 69 63 65 22 2c 22 74 6f 22 3a 22 62 6f 62 22 2c 22 61 6d 6f 75 6e 74 22 3a 31 30 30 7d
Full slot = header (32 B) + payload (23 B) + 201 B zero padding = 256 B
```

Go `wire_test.go` and Rust `wire.rs` must parse this exact header prefix and round-trip the full 256 B slot. If this fixture fails, the wire contract is broken.

## 4. Versioning

- `magic = 0x534C5257`, `version = 1`. Reader rejects file with mismatched magic/version.
- Future changes bump `version` and add a new fixture; old readers reject new version (fail-closed).

## 5. References

- Spec §6 wire layout and §8 backpressure/WAL notes in `docs/superpowers/specs/2026-08-22-tracer-bullet-design.md`.
- Implementations: `gateway/internal/ipc/wire.go`, `gateway/internal/ipc/ring.go`, `engine/src/wire.rs`, `engine/src/ring.rs`.
