mod grpc;
mod pg;
mod ring;
mod saga;
mod settle;
mod tail;
mod wire;

use crate::grpc::pb::reconciliation_server::ReconciliationServer;
use crate::grpc::pb;
use crate::ring::RingReader;
use crate::tail::{FileTailer, TailError};
use crate::saga::{SagaState, SagaStore, StoreError, TYPE_COMPENSATE, TYPE_SAGA_START, TYPE_STEP_FAIL, TYPE_STEP_OK};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::{Request, Response};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

// Polling cadences in one place: consumer backoff, sweep rhythm, PG mirror.
const RETRY_WAIT: Duration = Duration::from_secs(1);
const EMPTY_POLL: Duration = Duration::from_millis(1);
const ERROR_BACKOFF: Duration = Duration::from_millis(100);
const SWEEP_EVERY: Duration = Duration::from_secs(5);
const PG_FLUSH_EVERY: Duration = Duration::from_secs(2);
const PG_RETRY_EVERY: Duration = Duration::from_secs(10);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = PathBuf::from(env_or("WAL_PATH", "data/wal.log"));
    // Loopback by default: the status API has no auth, so never expose it
    // without deciding to (ENGINE_ADDR override).
    let addr = env_or("ENGINE_ADDR", "127.0.0.1:50051");

    // 1. Replay WAL into saga state (RPO = last fsynced record)
    let replay_start = Instant::now();
    let (store, stats) = match SagaStore::open(wal_path.clone()) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("engine: WAL replay failed: {e} — refusing to invent state, exiting");
            std::process::exit(1);
        }
    };
    println!(
        "engine: WAL replay done in {:?} ({} entries, corrupt_tail={})",
        replay_start.elapsed(),
        stats.entries_replayed,
        stats.wal_corrupt_tail_total
    );
    if stats.wal_corrupt_tail_total > 0 {
        eprintln!(
            "engine: WARNING {} corrupted WAL tail line(s) truncated — operator review required",
            stats.wal_corrupt_tail_total
        );
    }

    let store = Arc::new(Mutex::new(store));

    // 2. Event consumer task (polls; sleeps when empty). TRANSPORT=file is the
    // Windows demo path (JSONL tail with persisted offset); shm is the frozen
    // WSL2 bench path.
    let transport = env_or("TRANSPORT", "file");
    println!("engine: transport={transport}");
    let consumer_store = Arc::clone(&store);
    let consumer = tokio::task::spawn_blocking(move || {
        if transport == "file" {
            let events = PathBuf::from(
                std::env::var("EVENTS_PATH").unwrap_or_else(|_| "data/events.log".to_string()),
            );
            let offset = PathBuf::from(
                std::env::var("EVENTS_OFFSET")
                    .unwrap_or_else(|_| "data/events.offset".to_string()),
            );
            let flag = PathBuf::from(
                std::env::var("PARTITION_FLAG")
                    .unwrap_or_else(|_| "data/partition.flag".to_string()),
            );
            run_file_consumer(events, offset, flag, consumer_store)
        } else {
            let ring_path = PathBuf::from(
                std::env::var("RING_PATH")
                    .unwrap_or_else(|_| "/dev/shm/secureledger.ring".to_string()),
            );
            run_ring_consumer(ring_path, consumer_store)
        }
    });

    // Settlement sweep: every 5s ingest new CSVs from SETTLEMENT_DIR, match
    // against saga snapshots, annotate steps, and rewrite the report file the
    // gateway serves at GET /report. File-drop keeps the no-proto-change rule.
    let sweep_store = Arc::clone(&store);
    let settlement_dir = PathBuf::from(env_or("SETTLEMENT_DIR", "data/settlement"));
    let report_path = PathBuf::from(env_or("REPORT_PATH", "data/match-report.json"));
    tokio::task::spawn_blocking(move || {
        run_settlement_sweep(settlement_dir, report_path, sweep_store)
    });

    // Postgres mirror: queryable truth behind the WAL. Empty DATABASE_URL
    // skips PG entirely; unreachable PG degrades (WAL truth keeps serving)
    // with reconnect retries and full backfill on success (AP-explicit).
    let pg_store = Arc::clone(&store);
    let pg_url = env_or("DATABASE_URL", "");
    tokio::spawn(async move {
        if pg_url.is_empty() {
            println!("engine: pg disabled (DATABASE_URL unset)");
            return;
        }
        loop {
            match crate::pg::PgSink::connect(&pg_url).await {
                Ok((sink, _conn)) => {
                    if let Err(e) = sink.ensure_schema().await {
                        eprintln!("engine: pg degraded (schema: {e}); retry in 10s");
                        tokio::time::sleep(PG_RETRY_EVERY).await;
                        continue;
                    }
                    println!("engine: pg mirror live");
                    loop {
                        tokio::time::sleep(PG_FLUSH_EVERY).await;
                        let snaps: Vec<crate::saga::Saga> = match pg_store.lock() {
                            Ok(g) => g.sagas().cloned().collect(),
                            Err(_) => return, // poisoned
                        };
                        let mut failed = 0u32;
                        for s in &snaps {
                            if sink.upsert_saga(s).await.is_err() {
                                failed += 1;
                                break;
                            }
                        }
                        if failed > 0 {
                            eprintln!("engine: pg degraded (write failed); reconnecting");
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("engine: pg degraded (connect: {e}); retry in 10s");
                    tokio::time::sleep(PG_RETRY_EVERY).await;
                }
            }
        }
    });

    // 3. gRPC status server — GrpcBridge shares the same Arc'd store as the
    // ring consumer: one source of truth in-process.
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let bound = listener.local_addr()?;
    println!("engine: gRPC serving on {bound}");
    let graceful = tonic::transport::Server::builder()
        .add_service(ReconciliationServer::new(GrpcBridge {
            store: Arc::clone(&store),
        }))
        .serve_with_shutdown(bound, async {
            let _ = tokio::signal::ctrl_c().await;
            println!("engine: shutdown signal");
        });

    graceful.await?;
    let _ = consumer.await;
    Ok(())
}

/// Adapter so the tonic service reads from the same Arc<Mutex<SagaStore>> the
/// ring consumer writes to — one source of truth in-process.
struct GrpcBridge {
    store: Arc<Mutex<SagaStore>>,
}

#[tonic::async_trait]
impl crate::grpc::pb::reconciliation_server::Reconciliation for GrpcBridge {
    async fn get_saga_status(
        &self,
        request: Request<pb::GetSagaStatusRequest>,
    ) -> Result<Response<pb::GetSagaStatusResponse>, tonic::Status> {
        let id = request.into_inner().saga_id;
        if id.is_empty() {
            return Err(tonic::Status::invalid_argument("saga_id required"));
        }
        let guard = self
            .store
            .lock()
            .map_err(|_| tonic::Status::internal("store poisoned"))?;
        match guard.get(&id) {
            Some(saga) => Ok(Response::new(pb::GetSagaStatusResponse {
                saga_id: saga.saga_id.clone(),
                state: saga.state.to_string(),
                steps: saga.steps.clone(),
                updated_at: saga.updated_at_ns.to_string(),
            })),
            None => Err(tonic::Status::not_found(format!("saga {id} not found"))),
        }
    }
}

fn run_file_consumer(
    events: PathBuf,
    offset: PathBuf,
    partition_flag: PathBuf,
    store: Arc<Mutex<SagaStore>>,
) {
    let mut apart = false; // log partition transitions once, not every second
    let mut tail = loop {
        match FileTailer::open(events.clone(), offset.clone()) {
            Ok(t) => break t,
            Err(e) => {
                eprintln!("engine: events file not ready ({e}), retrying in 1s");
                std::thread::sleep(RETRY_WAIT);
            }
        }
    };
    loop {
        // Partition simulation (AIR D channel stop/replay): flag present =
        // intake paused, offsets frozen; removal = catch-up replay. The WAL
        // keeps every accepted event, so no state is invented or lost.
        if partition_flag.exists() {
            if !apart {
                eprintln!("engine: partition ON — intake paused");
                apart = true;
            }
            std::thread::sleep(RETRY_WAIT);
            continue;
        } else if apart {
            eprintln!("engine: partition OFF — catch-up replay");
            apart = false;
        }
        match tail.next() {
            Ok(Some(e)) => {
                let mut guard = match store.lock() {
                    Ok(g) => g,
                    Err(_) => return, // poisoned
                };
                if let Err(e2) = guard.apply(e.typ, &e.saga_id, &e.payload) {
                    eprintln!("engine: apply error type={:#x}: {e2}", e.typ);
                    // consumed; keep going (same rule as ring consumer)
                }
            }
            Ok(None) => std::thread::sleep(EMPTY_POLL),
            Err(TailError::CorruptLine(n)) => {
                eprintln!("engine: corrupt file line {n}; skipped, offset advanced");
            }
            Err(e) => {
                eprintln!("engine: tail read error: {e}");
                std::thread::sleep(ERROR_BACKOFF);
            }
        }
    }
}

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn run_settlement_sweep(dir: PathBuf, report: PathBuf, store: Arc<Mutex<SagaStore>>) {
    // Re-sweep every file every cycle: intake (loader) and settlement (CSV)
    // race, so a one-shot match would orphan rows whose sagas arrive later.
    // Re-matching is idempotent — step notes dedupe, the report rewrites.
    let mut seen: Vec<PathBuf> = Vec::new();
    loop {
        std::thread::sleep(SWEEP_EVERY);
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.extension().and_then(|e| e.to_str()) == Some("csv")
                    && !seen.contains(&path)
                {
                    seen.push(path);
                }
            }
        }
        if seen.is_empty() {
            continue; // no settlement dropped yet; retry
        }
        let mut rows: Vec<crate::settle::SettlementRow> = Vec::new();
        let mut corrupt = 0u64;
        for path in &seen {
            match std::fs::read_to_string(path) {
                Ok(text) => {
                    let (mut r, c) = crate::settle::parse_csv(&text);
                    rows.append(&mut r);
                    corrupt += c;
                }
                Err(e) => eprintln!("engine: settlement {} unreadable: {e}", path.display()),
            }
        }
        let mut guard = match store.lock() {
            Ok(g) => g,
            Err(_) => return, // poisoned
        };
        let snaps: Vec<crate::settle::SagaSnap> = guard
            .sagas()
            .map(|s| crate::settle::SagaSnap {
                saga_id: s.saga_id.clone(),
                txid: s.txid.clone(),
                amount: s.amount,
                updated_at_ns: s.updated_at_ns,
            })
            .collect();
        let total = snaps.len();
        let outcome = crate::settle::match_all(&snaps, &rows);
        for (sid, note) in &outcome.notes {
            guard.annotate(sid, note.clone());
        }
            let mut rep = outcome.report;
            rep.csv_corrupt += corrupt;
            drop(guard);
            // Deterministic report bytes: saga iteration order is HashMap
            // random per process, so sort the id lists — reports must diff
            // cleanly across restarts (recovery drill compares them).
            rep.missing.sort();
            rep.orphan.sort();
            rep.mismatch.sort();
        let doc = serde_json::json!({ "generated_at_ns": now_ns(), "report": rep });
        if let Some(parent) = report.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        match std::fs::write(&report, serde_json::to_string_pretty(&doc).unwrap()) {
            Ok(()) => println!(
                "engine: settlement sweep over {} sagas: {} exact + {} tolerated, {}/{}/{} missing/orphan/mismatch",
                total,
                rep.settled,
                rep.tolerated,
                rep.missing.len(),
                rep.orphan.len(),
                rep.mismatch.len()
            ),
            Err(e) => eprintln!("engine: report write failed: {e}"),
        }
    }
}

fn run_ring_consumer(ring_path: PathBuf, store: Arc<Mutex<SagaStore>>) {
    let mut reader = loop {
        match RingReader::open(&ring_path) {
            Ok(r) => break r,
            Err(e) => {
                eprintln!("engine: ring not ready ({e}), retrying in 1s");
                std::thread::sleep(RETRY_WAIT);
            }
        }
    };
    loop {
        match reader.next() {
            Ok(Some(slot)) => {
                let mut guard = match store.lock() {
                    Ok(g) => g,
                    Err(_) => return, // poisoned
                };
                let res = match slot.typ {
                    TYPE_SAGA_START | TYPE_STEP_OK | TYPE_STEP_FAIL | TYPE_COMPENSATE => {
                        guard.apply(slot.typ, &slot.saga_id, &slot.payload)
                    }
                    other => Err(StoreError::BadTransition(
                        format!("type_{other:x}"),
                        SagaState::InProgress,
                    )),
                };
                match res {
                    Ok(true) => {}
                    Ok(false) => {} // duplicate / idempotent re-delivery
                    Err(e) => {
                        eprintln!("engine: apply error type={:#x}: {e}", slot.typ);
                        // corrupt/unknown event already consumed; keep going
                    }
                }
            }
            Ok(None) => std::thread::sleep(EMPTY_POLL),
            Err(ring::RingError::CorruptSlot(idx, why)) => {
                eprintln!("engine: corrupt slot at {idx}: {why}");
                // tail already advanced inside reader
            }
            Err(e) => {
                eprintln!("engine: ring read error: {e}");
                std::thread::sleep(ERROR_BACKOFF);
            }
        }
    }
}
