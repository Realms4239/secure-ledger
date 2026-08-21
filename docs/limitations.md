# Limitations — Slice #1 Tracer Bullet

This is an experimental prototype. The following are known ceilings, not bugs.

- **Ring headroom:** 1M slots × 256 B = 256 MB in `/dev/shm` ≈100s @10k RPS. 24h @10k RPS ≈221 GB. The ring is a short backpressure buffer / benchmark path, not durable storage. Ring-full → honest `503 Service Unavailable` (no silent drop).
- **Per-record fsync cost:** Engine WAL does `fsync` per record (strong durability, RPO = last record, seconds) at the cost of throughput/latency. This conflicts with <10ms p99 @10k RPS and >50k RPS targets until measured. Future slices may use group/batch commit (higher throughput, larger RPO).
- **JWT HS256 + single policy:** Gateway uses HS256 with one rule (`subject == body.from_account`). This is a JWT-authenticated prototype, not Zero Trust (no PDP/PIP split, OIDC/JWKS, mTLS, device identity, WAF). Key rotation is a documented limitation in slice #1.
- **HashMap + WAL is not a queryable source-of-truth:** Engine keeps sagas in an in-mem `HashMap` with an append-only WAL (`data/wal.log`). Postgres as queryable source-of-truth is planned for slice #2.
- **Shm is WSL2/Linux bench path, not prod transport:** POSIX shared memory (`/dev/shm/secureledger.ring`, `syscall.Mmap` / `memmap2`) is for co-located Go↔Rust on WSL2/Linux. Production integration uses Postgres outbox / Kafka / NATS / SFTP adapters.
- **Idempotency-Key is mandatory but in-mem in slice #1:** `Idempotency-Key` header (UUID) is required; gateway dedup is an in-mem map with mutex. Cross-restart dedup requires Postgres unique index (slice #2); engine double-checks on consume.
