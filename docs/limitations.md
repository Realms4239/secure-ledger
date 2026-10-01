# Limitations — Tracer Bullet + Showcase

This is an experimental prototype. The following are known ceilings, not bugs.

- **Ring headroom:** 1M slots × 256 B = 256 MB in `/dev/shm` ≈100s @10k RPS. 24h @10k RPS ≈221 GB. The ring is a short backpressure buffer / benchmark path, not durable storage. Ring-full → honest `503 Service Unavailable` (no silent drop).
- **Per-record fsync cost:** Engine WAL does `fsync` per record (strong durability, RPO = last record, seconds) at the cost of throughput/latency. This conflicts with <10ms p99 @10k RPS and >50k RPS targets until measured. Future slices may use group/batch commit (higher throughput, larger RPO). The file-tail offset fsyncs every 100 events (replay ≤100 lines on crash, dedup-absorbed).
- **JWT HS256 + single policy:** Gateway uses HS256 with one rule (`subject == body.from_account`). This is a JWT-authenticated prototype, not Zero Trust (no PDP/PIP split, OIDC/JWKS, mTLS, device identity, WAF). Key rotation is a documented limitation.
- **HashMap + WAL with Postgres mirror:** Engine keeps sagas in an in-mem `HashMap` with an append-only WAL; Postgres mirrors for queries and degrades openly when unreachable (`NoTls`, local loopback — TLS terminates before any real deployment).
- **Shm is WSL2/Linux bench path, not prod transport:** POSIX shared memory is for co-located Go↔Rust on WSL2/Linux. The file transport (JSONL tail) is the Windows demo path and the outbox-pattern stand-in; production uses Postgres outbox / Kafka / NATS / SFTP adapters.
- **Idempotency-Key is mandatory but gateway dedup is in-mem:** `Idempotency-Key` header (UUID) is required; gateway dedup is an in-mem map (cross-restart dedup needs the planned Postgres unique index); engine double-checks on consume, so file replays are safe.
- **Fraud rules v1 are static:** velocity/amount-table/geo with in-mem windows; no ML, no per-account history beyond the window, ages default to established unless set.
- **Stress scope:** one recorded run (10k transfers, 10-min-class window) on dev hardware — no 24h sustained, no 50k RPS claim. Numbers live in `bench/results/` with hardware notes or they don't exist.
