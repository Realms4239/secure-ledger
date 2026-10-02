use crate::wire::{parse_slot, WireError, MAGIC, VERSION};
use memmap2::MmapMut;
use std::fs::OpenOptions;
use std::path::Path;

const HEADER_SIZE: u64 = 256; // normative per ipc-wire.md §1
const OFF_HEAD: u64 = 0;
const OFF_TAIL: u64 = 64;

#[derive(Debug)]
pub struct Slot {
    pub typ: u8,
    pub saga_id: [u8; 16],
    pub payload: Vec<u8>,
}

pub struct RingReader {
    mmap: MmapMut,
    #[allow(dead_code)] // kept for introspection/tests
    capacity: u64,
    mask: u64,
}

fn read_u64_le(mmap: &MmapMut, off: usize) -> u64 {
    let mut b = [0u8; 8];
    b.copy_from_slice(&mmap[off..off + 8]);
    u64::from_le_bytes(b)
}

impl RingReader {
    pub fn open(path: &Path) -> Result<Self, RingError> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(path)
            .map_err(RingError::Io)?;
        let mmap = unsafe { MmapMut::map_mut(&file).map_err(RingError::Io)? };
        if mmap.len() < HEADER_SIZE as usize {
            return Err(RingError::TooSmall);
        }
        let magic = u32::from_le_bytes(mmap[136..140].try_into().unwrap());
        let version = u16::from_le_bytes(mmap[140..142].try_into().unwrap());
        if magic != MAGIC {
            return Err(RingError::MagicMismatch(magic));
        }
        if version != VERSION {
            return Err(RingError::VersionMismatch(version));
        }
        let capacity = read_u64_le(&mmap, 128);
        if capacity == 0 || capacity & (capacity - 1) != 0 {
            return Err(RingError::BadCapacity(capacity));
        }
        Ok(Self {
            mmap,
            capacity,
            mask: capacity - 1,
        })
    }

    /// Poll one slot. None when empty. Corrupt slots advance tail and are
    /// returned as Err(CorruptSlot) — caller counts and moves on.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<Slot>, RingError> {
        let head = read_u64_le(&self.mmap, OFF_HEAD as usize);
        let tail = read_u64_le(&self.mmap, OFF_TAIL as usize);
        if tail == head {
            return Ok(None);
        }
        let idx = (tail & self.mask) as usize;
        let base = HEADER_SIZE as usize + idx * crate::wire::SLOT_SIZE;
        let end = base + crate::wire::SLOT_SIZE;
        let slot_bytes = self.mmap[base..end].to_vec();
        // Advance tail first thing we own; parse errors must not stall the ring.
        let new_tail = tail.wrapping_add(1);
        self.mmap[OFF_TAIL as usize..OFF_TAIL as usize + 8]
            .copy_from_slice(&new_tail.to_le_bytes());
        match parse_slot(&slot_bytes) {
            Ok((typ, saga_id, payload)) => Ok(Some(Slot {
                typ,
                saga_id,
                payload,
            })),
            Err(e @ WireError::BadPayloadLen(_)) => {
                // corrupt slot: skip, caller counts via error
                Err(RingError::CorruptSlot(tail, e.to_string()))
            }
            Err(_) => Err(RingError::CorruptSlot(tail, "bad slot".into())),
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RingError {
    #[error("io: {0}")]
    Io(#[source] std::io::Error),
    #[error("ring file too small")]
    TooSmall,
    #[error("magic mismatch: {0:#x}")]
    MagicMismatch(u32),
    #[error("version mismatch: {0}")]
    VersionMismatch(u16),
    #[error("capacity not power-of-two: {0}")]
    BadCapacity(u64),
    #[error("corrupt slot at {0}: {1}")]
    CorruptSlot(u64, String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{marshal_slot, SLOT_SIZE};

    fn write_header(mmap: &mut MmapMut, capacity: u64, head: u64) {
        mmap[..256].fill(0);
        mmap[OFF_HEAD as usize..8].copy_from_slice(&head.to_le_bytes());
        mmap[128..136].copy_from_slice(&capacity.to_le_bytes());
        mmap[136..140].copy_from_slice(&MAGIC.to_le_bytes());
        mmap[140..142].copy_from_slice(&VERSION.to_le_bytes());
    }

    #[test]
    fn reads_one_slot_then_none() {
        let dir = std::env::temp_dir();
        let path = dir.join(format!("test-engine-ring-{}.ring", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(HEADER_SIZE + 64 * SLOT_SIZE as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };

        write_header(&mut mmap, 64, 0);
        // write one SAGA_START slot at idx 0
        let saga_id = hex::decode("550e8400e29b41d4a716446655440000")
            .unwrap()
            .try_into()
            .unwrap();
        let mut slot = vec![0u8; SLOT_SIZE];
        marshal_slot(&mut slot, 0x01, saga_id, br#"{"k":1}"#).unwrap();
        let base = HEADER_SIZE as usize;
        mmap[base..base + SLOT_SIZE].copy_from_slice(&slot);
        // publish head=1 like the Go writer does after writing slot data
        mmap[OFF_HEAD as usize..8].copy_from_slice(&1u64.to_le_bytes());

        let mut reader = RingReader::open(&path).unwrap();
        let s = reader.next().unwrap().expect("one slot");
        assert_eq!(s.typ, 0x01);
        assert_eq!(s.saga_id, saga_id);
        assert_eq!(s.payload, br#"{"k":1}"#.to_vec());
        // tail persisted so writer sees progress
        assert_eq!(read_u64_le(&reader.mmap, OFF_TAIL as usize), 1);
        assert!(reader.next().unwrap().is_none());

        drop(reader);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn bad_magic_rejected() {
        let path = std::env::temp_dir().join(format!("test-badmagic-{}.ring", std::process::id()));
        let _ = std::fs::remove_file(&path);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        file.set_len(HEADER_SIZE + 64 * SLOT_SIZE as u64).unwrap();
        let mut mmap = unsafe { MmapMut::map_mut(&file).unwrap() };
        write_header(&mut mmap, 64, 0);
        mmap[136] ^= 0xFF;
        match RingReader::open(&path) {
            Err(RingError::MagicMismatch(_)) => {}
            other => panic!("want MagicMismatch, got {:?}", other.map(|_| ())),
        }
        drop(mmap);
        let _ = std::fs::remove_file(&path);
    }
}
