# Architecture

```
seed/loader ──POST /transfers──► Go Gateway :8080 ──TRANSPORT=file──► data/events.log ──tail──► Rust Engine
  (JWT, Idempotency-Key,          │  policy, fraud plugin, audit log                        │  saga DFA + WAL ──► Postgres mirror
   operator, txid)                │  /settlement/upload ──► data/settlement/*.csv ──sweep──► │  match report ──► data/match-report.json
                                  │  /dashboard + /events/stream (SSE) ◄── hub               │  gRPC :50051 GetSagaStatus
                                  └──────────────────────────────────────────────────────────┘
```

## Edge (Go, `gateway/`)

- `internal/authn` — JWT HS256 (`exp`/`nbf`, `sub`); `internal/policy` — one
  rule (`subject == from_account`); `internal/idempotency` — in-mem
  key→saga map (409 on retry, answered before fraud scoring).
- `internal/fraud` — Project A rules v1: velocity (>5/min), new-account
  amount table, cross-operator geo-impossibility. Holds return 202
  `held_for_review` and never reach the sink.
- `internal/ipc` — `Sink` interface; `RingWriter` (frozen shm bench path);
  `FileSink` (JSONL append, the demo path).
- `internal/dash` — embedded dashboard + SSE hub (replay-8, drop-slowest).
- `internal/handler` — routes: transfers, fail-injection, sagas (gRPC proxy),
  settlement upload (file-drop), report (serves engine file), chaos
  partition flag (file-drop), metricsz, healthz.

## Core (Rust, `engine/`)

- `tail.rs` — file consumer with persisted offset (fsync every 100);
  `ring.rs` — frozen shm reader; `saga.rs` — DFA + WAL (fsync/record) +
  txid index; `settle.rs` — CSV matcher (exact → fee/skew tolerance →
  missing/orphan/mismatch) with 5s idempotent re-sweeps;
  `pg.rs` — Postgres mirror with degraded mode; `grpc.rs` — status server.
- Partition simulation: `PARTITION_FLAG` present = intake paused, offsets
  frozen; removal = catch-up replay. Crash recovery: WAL replay on boot
  (RPO = last record, 500 entries in ~30ms measured).

## Data flow guarantees

- Nothing unauthenticated or non-idempotent reaches any transport.
- Holds and rejects never enter the ledger; audit + counters carry them.
- Every accepted intake is WAL'd before it is observable; offsets make file
  replay deterministic; reports sort id lists for byte-stable diffs.
- Engine truth survives PG loss (degraded + backfill) and kill-9 (replay).
