use thiserror::Error;

pub const SLOT_SIZE: usize = 256;
pub const PAYLOAD_MAX: usize = 224;
pub const MAGIC: u32 = 0x534C_5257;
pub const VERSION: u16 = 1;

// Slot layout per proto/ipc-wire.md §2 (256 B, LE):
// 0     type          u8
// 1     flags         u8 (0)
// 2     reserved      u16 LE (0)
// 4     saga_id       [u8;16]
// 20    timestamp_ns  u64 LE
// 28    payload_len   u16 LE
// 30    _pad          u16 LE (0)
// 32    payload       [224] zero-padded

#[derive(Debug, Error)]
pub enum WireError {
    #[error("slot must be {SLOT_SIZE} bytes, got {0}")]
    BadLength(usize),
    #[error("payload too large: {0} > {PAYLOAD_MAX}")]
    #[allow(dead_code)]
    PayloadTooLarge(usize),
    #[error("payload_len too large in slot: {0}")]
    BadPayloadLen(usize),
}

#[allow(dead_code)]
fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

#[allow(dead_code)]
pub fn marshal_slot(
    buf: &mut [u8],
    typ: u8,
    saga_id: [u8; 16],
    payload: &[u8],
) -> Result<(), WireError> {
    if buf.len() != SLOT_SIZE {
        return Err(WireError::BadLength(buf.len()));
    }
    if payload.len() > PAYLOAD_MAX {
        return Err(WireError::PayloadTooLarge(payload.len()));
    }
    buf.fill(0);
    buf[0] = typ;
    buf[4..20].copy_from_slice(&saga_id);
    buf[20..28].copy_from_slice(&now_ns().to_le_bytes());
    buf[28..30].copy_from_slice(&(payload.len() as u16).to_le_bytes());
    buf[32..32 + payload.len()].copy_from_slice(payload);
    Ok(())
}

pub fn parse_slot(slot: &[u8]) -> Result<(u8, [u8; 16], Vec<u8>), WireError> {
    if slot.len() != SLOT_SIZE {
        return Err(WireError::BadLength(slot.len()));
    }
    let typ = slot[0];
    let mut saga_id = [0u8; 16];
    saga_id.copy_from_slice(&slot[4..20]);
    let payload_len = u16::from_le_bytes([slot[28], slot[29]]) as usize;
    if payload_len > PAYLOAD_MAX {
        return Err(WireError::BadPayloadLen(payload_len));
    }
    Ok((typ, saga_id, slot[32..32 + payload_len].to_vec()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture_saga_id() -> [u8; 16] {
        // 550e8400-e29b-41d4-a716-446655440000 raw bytes
        hex::decode("550e8400e29b41d4a716446655440000")
            .unwrap()
            .try_into()
            .unwrap()
    }

    #[test]
    fn round_trip() {
        let saga_id = fixture_saga_id();
        let payload = br#"{"from":"alice","to":"bob","amount":100}"#;
        let mut buf = vec![0u8; SLOT_SIZE];
        marshal_slot(&mut buf, 0x01, saga_id, payload).unwrap();
        assert_eq!(buf.len(), SLOT_SIZE);
        let (typ, got_id, got_payload) = parse_slot(&buf).unwrap();
        assert_eq!(typ, 0x01);
        assert_eq!(got_id, saga_id);
        assert_eq!(got_payload, payload.to_vec());
        // zero-padding beyond payload
        assert!(buf[32 + payload.len()..].iter().all(|&b| b == 0));
    }

    #[test]
    fn payload_too_large_err() {
        let large = vec![b'x'; PAYLOAD_MAX + 1];
        let mut buf = vec![0u8; SLOT_SIZE];
        assert!(marshal_slot(&mut buf, 0x01, [0u8; 16], &large).is_err());
        // oversized declared len on parse
        let mut bad = vec![0u8; SLOT_SIZE];
        bad[28..30].copy_from_slice(&(PAYLOAD_MAX as u16 + 1).to_le_bytes());
        assert!(parse_slot(&bad).is_err());
    }

    #[test]
    fn bad_length_err() {
        assert!(parse_slot(&[0u8; 255]).is_err());
    }

    #[test]
    fn fixture_parses_go_written_slot() {
        // Fixture from proto/ipc-wire.md §3 as a Go writer would produce it:
        // type=0x01, saga_id=550e8400-...-0000, ts=1700000000000000000,
        // payload_len=40 (0x28), payload={"from":"alice","to":"bob","amount":100}
        let mut slot = vec![0u8; SLOT_SIZE];
        slot[0] = 0x01;
        slot[4..20].copy_from_slice(&fixture_saga_id());
        slot[20..28].copy_from_slice(&1_700_000_000_000_000_000u64.to_le_bytes());
        slot[28..30].copy_from_slice(&40u16.to_le_bytes());
        slot[32..72].copy_from_slice(br#"{"from":"alice","to":"bob","amount":100}"#);
        let (typ, saga_id, payload) = parse_slot(&slot).unwrap();
        assert_eq!(typ, 0x01);
        assert_eq!(saga_id, fixture_saga_id());
        assert_eq!(
            std::str::from_utf8(&payload).unwrap(),
            r#"{"from":"alice","to":"bob","amount":100}"#
        );
        // LE timestamp check: bytes at 20..28 must be 00 00 2a 36 fe 9c 97 17
        assert_eq!(
            &slot[20..28],
            &hex::decode("00002a36fe9c9717").unwrap()[..]
        );
    }
}
