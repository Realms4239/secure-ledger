use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;

pub const TYPE_SAGA_START: u8 = 0x01;
pub const TYPE_STEP_OK: u8 = 0x02;
pub const TYPE_STEP_FAIL: u8 = 0x03;
pub const TYPE_COMPENSATE: u8 = 0x04;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SagaState {
    InProgress,
    Compensating,
    Compensated,
}

impl std::fmt::Display for SagaState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            SagaState::InProgress => "InProgress",
            SagaState::Compensating => "Compensating",
            SagaState::Compensated => "Compensated",
        };
        f.write_str(s)
    }
}

#[derive(Debug, Clone)]
pub struct Saga {
    pub saga_id: String,
    pub state: SagaState,
    pub steps: Vec<String>,
    pub idempotency_key: String,
    pub updated_at_ns: u64,
    pub operator: String,
    pub txid: String,
    pub to: String,
    pub amount: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Payload {
    from: String,
    #[serde(default)]
    to: String,
    #[serde(default)]
    amount: i64,
    idempotency_key: String,
    #[serde(default)]
    operator: String,
    #[serde(default)]
    txid: String,
}

// WAL line format (one JSON object per line):
// {"saga_id":"uuid","idempotency_key":"...","typ":1,"state":"in_progress","ts":<ns>,
//  "operator":"mvola","txid":"...","to":"bob","amount":100}
// operator/txid/to/amount were added for settlement matching; old WAL lines
// without them replay with defaults (empty/0) via serde(default).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WalEntry {
    pub saga_id: String,
    pub idempotency_key: String,
    pub typ: u8,
    pub state: String,
    pub ts: u64,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub txid: String,
    #[serde(default)]
    pub to: String,
    #[serde(default)]
    pub amount: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("wal io: {0}")]
    WalIo(#[from] std::io::Error),
    #[error("wal json: {0}")]
    WalJson(#[from] serde_json::Error),
    #[error("unknown saga {0}")]
    UnknownSaga(String),
    #[error("bad transition {0} from {1}")]
    BadTransition(String, SagaState),
}

pub struct SagaStore {
    sagas: HashMap<String, Saga>,
    dedup: HashMap<String, String>, // idempotency_key -> saga_id
    txids: HashMap<String, String>, // operator txid -> saga_id (settlement join key)
    wal: Wal,
    pub duplicate_events_total: u64,
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn saga_id_to_string(id: &[u8; 16]) -> String {
    // Canonical RFC 4122 display form so gateway uuid.New() strings match:
    // 8-4-4-4-12 lowercase hex.
    let h: String = id.iter().map(|b| format!("{b:02x}")).collect();
    format!(
        "{}-{}-{}-{}-{}",
        &h[0..8],
        &h[8..12],
        &h[12..16],
        &h[16..20],
        &h[20..32]
    )
}

impl SagaStore {
    /// Open store with WAL at `path`, replaying it into state.
    pub fn open(wal_path: PathBuf) -> Result<(Self, ReplayStats), StoreError> {
        let wal = Wal::open(wal_path);
        let mut stats = ReplayStats::default();
        let entries = wal.replay(&mut stats.wal_corrupt_tail_total)?;
        stats.entries_replayed = entries.len();
        let mut sagas = HashMap::new();
        let mut dedup = HashMap::new();
        let mut txids = HashMap::new();
        for e in &entries {
            let state = parse_state(&e.state)?;
            let entry = sagas.entry(e.saga_id.clone()).or_insert_with(|| Saga {
                saga_id: e.saga_id.clone(),
                state,
                steps: Vec::new(),
                idempotency_key: e.idempotency_key.clone(),
                updated_at_ns: e.ts,
                operator: e.operator.clone(),
                txid: e.txid.clone(),
                to: e.to.clone(),
                amount: e.amount,
            });
            entry.state = state;
            entry.updated_at_ns = e.ts;
            if !entry.steps.contains(&format!("type_{}", e.typ)) {
                entry.steps.push(format!("type_{}", e.typ));
            }
            dedup.insert(e.idempotency_key.clone(), e.saga_id.clone());
            if !e.txid.is_empty() {
                txids.insert(e.txid.clone(), e.saga_id.clone());
            }
        }
        Ok((
            Self {
                sagas,
                dedup,
                txids,
                wal,
                duplicate_events_total: 0,
            },
            stats,
        ))
    }

    /// Apply a ring event. Returns Ok(true) if state changed, Ok(false) on
    /// duplicate (no new saga created). Every accepted transition is WAL'd
    /// with fsync before the caller observes success.
    pub fn apply(&mut self, typ: u8, saga_id: &[u8; 16], payload: &[u8]) -> Result<bool, StoreError> {
        let sid = saga_id_to_string(saga_id);
        match typ {
            TYPE_SAGA_START => {
                let p: Payload = serde_json::from_slice(payload)
                    .map_err(StoreError::WalJson)?;
                if let Some(existing) = self.dedup.get(&p.idempotency_key).cloned() {
                    // duplicate: no state change, no WAL write
                    self.duplicate_events_total += 1;
                    let _ = existing;
                    return Ok(false);
                }
                let entry = WalEntry {
                    saga_id: sid.clone(),
                    idempotency_key: p.idempotency_key.clone(),
                    typ,
                    state: "in_progress".into(),
                    ts: now_ns(),
                    operator: p.operator.clone(),
                    txid: p.txid.clone(),
                    to: p.to.clone(),
                    amount: p.amount,
                };
                self.wal.append(&entry)?;
                self.sagas.insert(
                    sid.clone(),
                    Saga {
                        saga_id: sid.clone(),
                        state: SagaState::InProgress,
                        steps: vec!["saga_start".into()],
                        idempotency_key: p.idempotency_key.clone(),
                        updated_at_ns: entry.ts,
                        operator: p.operator.clone(),
                        txid: p.txid.clone(),
                        to: p.to.clone(),
                        amount: p.amount,
                    },
                );
                self.dedup.insert(p.idempotency_key, sid.clone());
                if !p.txid.is_empty() {
                    self.txids.insert(p.txid, sid);
                }
                Ok(true)
            }
            TYPE_STEP_FAIL | TYPE_COMPENSATE => {
                let saga = self
                    .sagas
                    .get_mut(&sid)
                    .ok_or_else(|| StoreError::UnknownSaga(sid.clone()))?;
                match saga.state {
                    SagaState::InProgress => saga.state = SagaState::Compensating,
                    SagaState::Compensating => saga.state = SagaState::Compensated,
                    SagaState::Compensated => return Ok(false), // idempotent re-fail
                }
                saga.steps.push(format!("type_{typ}"));
                saga.updated_at_ns = now_ns();
                let entry = WalEntry {
                    saga_id: sid,
                    idempotency_key: saga.idempotency_key.clone(),
                    typ,
                    state: state_str(saga.state).to_string(),
                    ts: saga.updated_at_ns,
                    operator: saga.operator.clone(),
                    txid: saga.txid.clone(),
                    to: saga.to.clone(),
                    amount: saga.amount,
                };
                self.wal.append(&entry)?;
                Ok(true)
            }
            TYPE_STEP_OK => {
                let saga = self
                    .sagas
                    .get_mut(&sid)
                    .ok_or_else(|| StoreError::UnknownSaga(sid.clone()))?;
                saga.steps.push(format!("type_{typ}"));
                saga.updated_at_ns = now_ns();
                let entry = WalEntry {
                    saga_id: sid,
                    idempotency_key: saga.idempotency_key.clone(),
                    typ,
                    state: state_str(saga.state).to_string(),
                    ts: saga.updated_at_ns,
                    operator: saga.operator.clone(),
                    txid: saga.txid.clone(),
                    to: saga.to.clone(),
                    amount: saga.amount,
                };
                self.wal.append(&entry)?;
                Ok(true)
            }
            other => Err(StoreError::BadTransition(format!("type_{other:x}"), SagaState::InProgress)),
        }
    }

    pub fn get(&self, saga_id_hex: &str) -> Option<&Saga> {
        self.sagas.get(saga_id_hex)
    }

    pub fn sagas(&self) -> impl Iterator<Item = &Saga> {
        self.sagas.values()
    }

    /// Append a match note to a saga's steps (deduped; surfaced via status).
    pub fn annotate(&mut self, saga_id: &str, note: String) {
        if let Some(s) = self.sagas.get_mut(saga_id) {
            if !s.steps.contains(&note) {
                s.steps.push(note);
            }
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.sagas.len()
    }

    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.sagas.is_empty()
    }

}

fn state_str(s: SagaState) -> &'static str {
    match s {
        SagaState::InProgress => "in_progress",
        SagaState::Compensating => "compensating",
        SagaState::Compensated => "compensated",
    }
}

fn parse_state(s: &str) -> Result<SagaState, StoreError> {
    match s {
        "in_progress" => Ok(SagaState::InProgress),
        "compensating" => Ok(SagaState::Compensating),
        "compensated" => Ok(SagaState::Compensated),
        other => Err(StoreError::BadTransition(other.into(), SagaState::InProgress)),
    }
}

pub struct Wal {
    path: PathBuf,
    file: Option<std::fs::File>,
}

#[derive(Debug, Default, Clone)]
pub struct ReplayStats {
    pub wal_corrupt_tail_total: u64,
    pub entries_replayed: usize,
}

impl Wal {
    pub fn open(path: PathBuf) -> Self {
        Self { path, file: None }
    }

    /// Append one entry as a JSON line with fsync. Cost documented in spec §8.
    pub fn append(&mut self, entry: &WalEntry) -> Result<(), StoreError> {
        let file = match self.file.as_mut() {
            Some(f) => f,
            None => {
                if let Some(parent) = self.path.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                self.file
                    .insert(std::fs::OpenOptions::new().create(true).append(true).open(&self.path)?)
            }
        };
        let line = serde_json::to_string(entry)?;
        file.write_all(line.as_bytes())?;
        file.write_all(b"\n")?;
        file.sync_all()?; // fsync per record — spec §8 trade-off
        Ok(())
    }

    /// Read all valid entries. A corrupted final line is counted (not fatal);
    /// corrupted middle lines are skipped and counted too.
    pub fn replay(&self, corrupt_tail: &mut u64) -> Result<Vec<WalEntry>, StoreError> {
        let mut out = Vec::new();
        if !self.path.exists() {
            return Ok(out);
        }
        let f = std::fs::File::open(&self.path)?;
        for line in BufReader::new(f).lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            match serde_json::from_str::<WalEntry>(&line) {
                Ok(e) => out.push(e),
                Err(_) => *corrupt_tail += 1,
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_wal(name: &str) -> PathBuf {
        let dir = std::env::temp_dir();
        let p = dir.join(format!("{name}-{}.wal", std::process::id()));
        let _ = std::fs::remove_file(&p);
        p
    }

    fn saga_id(n: u8) -> [u8; 16] {
        let mut id = [0u8; 16];
        id[0] = n;
        id
    }

    fn payload(key: &str) -> Vec<u8> {
        serde_json::json!({"from":"alice","to":"bob","amount":100,"idempotency_key":key})
            .to_string()
            .into_bytes()
    }

    #[test]
    fn saga_start_creates_in_progress() {
        let wal_path = temp_wal("t1");
        let mut store = SagaStore::open(wal_path.clone()).unwrap().0;
        let changed = store
            .apply(TYPE_SAGA_START, &saga_id(1), &payload("key-a"))
            .unwrap();
        assert!(changed);
        let s = store.get(&saga_id_to_string(&saga_id(1))).unwrap();
        assert_eq!(s.state, SagaState::InProgress);
        assert_eq!(s.idempotency_key, "key-a");
        drop(store);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn duplicate_idempotency_key_no_new_saga() {
        let wal_path = temp_wal("t2");
        let mut store = SagaStore::open(wal_path.clone()).unwrap().0;
        let sid = [7u8; 16];
        store.apply(TYPE_SAGA_START, &sid, &payload("dup-key")).unwrap();
        // second event, different saga_id bytes but same idempotency key
        let mut sid2 = [7u8; 16];
        sid2[15] = 9;
        let changed = store.apply(TYPE_SAGA_START, &sid2, &payload("dup-key")).unwrap();
        assert!(!changed, "duplicate must not change state");
        assert_eq!(store.len(), 1, "no new saga on duplicate");
        assert_eq!(store.duplicate_events_total, 1);
        drop(store);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn step_fail_transitions_to_compensated() {
        let wal_path = temp_wal("t3");
        let mut store = SagaStore::open(wal_path.clone()).unwrap().0;
        let sid = saga_id(3);
        store.apply(TYPE_SAGA_START, &sid, &payload("key-c")).unwrap();
        store.apply(TYPE_STEP_FAIL, &sid, b"").unwrap();
        let s = store.get(&saga_id_to_string(&sid)).unwrap();
        assert_eq!(s.state, SagaState::Compensating);
        store.apply(TYPE_COMPENSATE, &sid, b"").unwrap();
        let s = store.get(&saga_id_to_string(&sid)).unwrap();
        assert_eq!(s.state, SagaState::Compensated);
        // re-fail after compensated is idempotent no-op
        let changed = store.apply(TYPE_STEP_FAIL, &sid, b"").unwrap();
        assert!(!changed);
        drop(store);
        let _ = std::fs::remove_file(wal_path);
    }

    #[test]
    fn wal_round_trip_and_corrupted_tail() {
        let wal_path = temp_wal("t4");
        {
            let mut store = SagaStore::open(wal_path.clone()).unwrap().0;
            store.apply(TYPE_SAGA_START, &saga_id(1), &payload("k1")).unwrap();
            store.apply(TYPE_SAGA_START, &saga_id(2), &payload("k2")).unwrap();
            store.apply(TYPE_STEP_FAIL, &saga_id(1), b"").unwrap();
            // simulate crash mid-write: append garbage directly
            use std::io::Write;
            let mut f = std::fs::OpenOptions::new().append(true).open(&wal_path).unwrap();
            f.write_all(b"{corrupted json line\n").unwrap();
        }
        let mut corrupt = 0u64;
        let entries = Wal::open(wal_path.clone()).replay(&mut corrupt).unwrap();
        assert_eq!(entries.len(), 3, "3 valid entries");
        assert_eq!(corrupt, 1, "one corrupted tail counted");

        // full reopen replays into correct states
        let (store, stats) = SagaStore::open(wal_path.clone()).unwrap();
        assert_eq!(stats.entries_replayed, 3);
        assert_eq!(stats.wal_corrupt_tail_total, 1);
        let s = store.get(&saga_id_to_string(&saga_id(1))).unwrap();
        assert_eq!(s.state, SagaState::Compensating); // FAIL applied, COMPENSATE not yet
        let s2 = store.get(&saga_id_to_string(&saga_id(2))).unwrap();
        assert_eq!(s2.state, SagaState::InProgress);
        drop(store);
        let _ = std::fs::remove_file(wal_path);
    }
}
