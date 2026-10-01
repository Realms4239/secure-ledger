use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum TailError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("json: {0}")]
    Json(#[from] serde_json::Error),
    #[error("bad saga id {0}")]
    BadId(String),
    #[error("corrupt line {0}")]
    CorruptLine(u64),
}

#[derive(Debug, Clone)]
pub struct TailEvent {
    pub typ: u8,
    pub saga_id: [u8; 16],
    pub payload: Vec<u8>,
}

fn parse_uuid_hex(s: &str) -> Result<[u8; 16], TailError> {
    let h: String = s.chars().filter(|c| *c != '-').collect();
    if h.len() != 32 {
        return Err(TailError::BadId(s.to_string()));
    }
    let mut id = [0u8; 16];
    for (i, b) in id.iter_mut().enumerate() {
        *b = u8::from_str_radix(&h[2 * i..2 * i + 2], 16)
            .map_err(|_| TailError::BadId(s.to_string()))?;
    }
    Ok(id)
}

/// File tailer over the gateway FileSink JSONL log. Offset = bytes consumed,
/// persisted to a sibling file after every event (replay resumes exactly).
pub struct FileTailer {
    file: std::fs::File,
    offset_path: PathBuf,
    offset: u64,
    line_no: u64,
    since_sync: u64,
}

impl FileTailer {
    pub fn open(events: PathBuf, offset: PathBuf) -> Result<Self, TailError> {
        let stored = read_offset(&offset);
        let mut file = OpenOptions::new().read(true).open(&events)?;
        let len = file.metadata()?.len();
        let pos = stored.min(len); // truncated/rotated log: clamp, never seek past EOF
        file.seek(SeekFrom::Start(pos))?;
        Ok(Self {
            file,
            offset_path: offset,
            offset: pos,
            line_no: 0,
            since_sync: 0,
        })
    }

    /// Next event, or None when at EOF (caller sleeps and polls again).
    /// A bad line is skipped (offset still advances) and reported as
    /// CorruptLine; the caller logs and continues, mirroring CorruptSlot.
    #[allow(clippy::should_implement_trait)]
    pub fn next(&mut self) -> Result<Option<TailEvent>, TailError> {
        let mut buf = Vec::new();
        let mut byte = [0u8; 1];
        let line_start = self.offset;
        loop {
            match self.file.read(&mut byte) {
                Ok(0) => {
                    if buf.is_empty() {
                        return Ok(None);
                    }
                    // Torn write: the writer crashed mid-line (no newline yet).
                    // Rewind and wait — consuming the fragment would lose the
                    // event when the rest arrives. Offset file untouched.
                    self.file.seek(SeekFrom::Start(line_start))?;
                    self.offset = line_start;
                    return Ok(None);
                }
                Ok(_) => {
                    self.offset += 1;
                    if byte[0] == b'\n' {
                        break;
                    }
                    buf.push(byte[0]);
                }
                Err(e) => return Err(TailError::Io(e)),
            }
        }
        self.line_no += 1;
        let line_no = self.line_no;
        let end = self.offset;
        let parse = (|| -> Result<TailEvent, TailError> {
            let v: serde_json::Value = serde_json::from_slice(&buf)?;
            let t = v
                .get("t")
                .and_then(|t| t.as_u64())
                .ok_or(TailError::CorruptLine(line_no))?;
            // Fail closed on out-of-range types: `as u8` would wrap 999 into
            // a valid-looking discriminant.
            if t > 255 {
                return Err(TailError::CorruptLine(line_no));
            }
            let typ = t as u8;
            let id = v
                .get("id")
                .and_then(|i| i.as_str())
                .ok_or(TailError::CorruptLine(line_no))?;
            let payload = v
                .get("p")
                .and_then(|p| p.as_str())
                .ok_or(TailError::CorruptLine(line_no))?;
            Ok(TailEvent {
                typ,
                saga_id: parse_uuid_hex(id)?,
                payload: payload.as_bytes().to_vec(),
            })
        })();
        // Advance durability past this line no matter what: a poison line
        // must not wedge the ledger (same rule as corrupt shm slots).
        // The offset write fsyncs every 100 events: a crash replays at most
        // 100 lines, and idempotent re-delivery is a dedup no-op — same
        // guarantee, half the fsync load of the WAL path.
        self.since_sync += 1;
        let sync = self.since_sync.is_multiple_of(100);
        persist_offset(&self.offset_path, end, sync)?;
        match parse {
            Ok(e) => Ok(Some(e)),
            Err(TailError::BadId(_)) => Err(TailError::CorruptLine(line_no)),
            Err(TailError::Json(_)) => Err(TailError::CorruptLine(line_no)),
            Err(e) => Err(e),
        }
    }
}

fn read_offset(path: &Path) -> u64 {
    let mut buf = [0u8; 8];
    let mut f = match OpenOptions::new().read(true).open(path) {
        Ok(f) => f,
        Err(_) => return 0,
    };
    match f.read_exact(&mut buf) {
        Ok(()) => u64::from_le_bytes(buf),
        Err(_) => 0,
    }
}

fn persist_offset(path: &Path, offset: u64, sync: bool) -> Result<(), TailError> {
    let mut f = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)?;
    f.write_all(&offset.to_le_bytes())?;
    if sync {
        f.sync_all()?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    #[test]
    fn tail_reads_one_event_then_none_and_resumes_from_offset() {
        let dir = std::env::temp_dir().join(format!("tailtest-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ev = dir.join("events.log");
        let off = dir.join("events.offset");
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(&ev).unwrap();
        writeln!(f, r#"{{"t":1,"id":"550e8400-e29b-41d4-a716-446655440000","ts":1700000000000000000,"p":"{{\"from\":\"alice\"}}"}}"#).unwrap();
        drop(f);
        let mut tail = FileTailer::open(ev.clone(), off.clone()).unwrap();
        let e = tail.next().unwrap().expect("one event");
        assert_eq!(e.typ, 0x01);
        assert_eq!(hex::encode(e.saga_id), "550e8400e29b41d4a716446655440000");
        assert!(tail.next().unwrap().is_none());
        drop(tail);
        // reopen: offset persisted, still none (no re-delivery)
        let mut tail2 = FileTailer::open(ev, off).unwrap();
        assert!(tail2.next().unwrap().is_none());
    }

    fn setup(name: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!("tailtest-{name}-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let ev = dir.join("events.log");
        let off = dir.join("events.offset");
        let _ = std::fs::remove_file(&ev);
        let _ = std::fs::remove_file(&off);
        (ev, off)
    }

    fn append(ev: &std::path::Path, s: &str) {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().create(true).append(true).open(ev).unwrap();
        f.write_all(s.as_bytes()).unwrap();
    }

    #[test]
    fn torn_write_control_single_append() {
        // Control: the identical bytes in ONE write must parse. If this
        // fails, the bytes are bad; if it passes, the rewind path is bad.
        let (ev, off) = setup("tornctl");
        append(&ev, "{\"t\":1,\"id\":\"550e8400-e29b-41d4-a716-446655440000\",\"ts\":1,\"p\":\"{\\\"from\\\":\\\"alice\\\"}}\"}\n");
        let mut tail = FileTailer::open(ev, off).unwrap();
        let e = tail.next().unwrap().expect("control event");
        assert_eq!(e.typ, 0x01);
    }

    #[test]
    fn torn_write_retries_partial_line() {
        let (ev, off) = setup("torn");
        // Crash mid-append: partial line, no newline — must NOT consume.
        append(&ev, r#"{"t":1,"id":"550e8400-e29b-41d4-a716-446655440000","ts":1,"p":"{"#);
        let mut tail = FileTailer::open(ev.clone(), off.clone()).unwrap();
        assert!(tail.next().unwrap().is_none(), "partial line must wait");
        // Writer completes the line — the full event parses exactly once.
        // Needed bytes: \"from\":\"alice\" + } (close p object) + " (close
        // p string) + } (close outer object) + newline.
        append(&ev, "\\\"from\\\":\\\"alice\\\"}\"}\n");
        let e = tail.next().unwrap().expect("completed event");
        assert_eq!(e.typ, 0x01);
        assert!(tail.next().unwrap().is_none());
    }

    #[test]
    fn event_type_out_of_range_rejected() {
        let (ev, off) = setup("rangecheck");
        append(&ev, "{\"t\":999,\"id\":\"550e8400-e29b-41d4-a716-446655440000\",\"ts\":1,\"p\":\"{}\"}\n");
        let mut tail = FileTailer::open(ev, off).unwrap();
        match tail.next() {
            Err(TailError::CorruptLine(1)) => {}
            other => panic!("want CorruptLine(1), got {other:?}"),
        }
    }
}
