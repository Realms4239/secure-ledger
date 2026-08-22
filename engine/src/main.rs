mod grpc;
mod ring;
mod saga;
mod wire;

use crate::grpc::pb::reconciliation_server::ReconciliationServer;
use crate::grpc::pb;
use crate::ring::RingReader;
use crate::saga::{SagaState, SagaStore, StoreError, TYPE_COMPENSATE, TYPE_SAGA_START, TYPE_STEP_FAIL, TYPE_STEP_OK};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tonic::{Request, Response};

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let wal_path = PathBuf::from(env_or("WAL_PATH", "data/wal.log"));
    let ring_path = PathBuf::from(env_or("RING_PATH", "/dev/shm/secureledger.ring"));
    let addr = env_or("ENGINE_ADDR", "0.0.0.0:50051");

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

    // 2. Ring consumer task (polls; sleeps when empty)
    let consumer_store = Arc::clone(&store);
    let ring_for_consumer = ring_path.clone();
    let consumer = tokio::task::spawn_blocking(move || {
        run_ring_consumer(ring_for_consumer, consumer_store)
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

fn run_ring_consumer(ring_path: PathBuf, store: Arc<Mutex<SagaStore>>) {
    let mut reader = loop {
        match RingReader::open(&ring_path) {
            Ok(r) => break r,
            Err(e) => {
                eprintln!("engine: ring not ready ({e}), retrying in 1s");
                std::thread::sleep(Duration::from_secs(1));
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
            Ok(None) => std::thread::sleep(Duration::from_millis(1)),
            Err(ring::RingError::CorruptSlot(idx, why)) => {
                eprintln!("engine: corrupt slot at {idx}: {why}");
                // tail already advanced inside reader
            }
            Err(e) => {
                eprintln!("engine: ring read error: {e}");
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}
