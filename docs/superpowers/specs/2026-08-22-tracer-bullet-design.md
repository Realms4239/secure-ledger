# Spec: Secure Ledger — Tracer Bullet + Showcase Roadmap

**Date:** 2026-08-22
**Status:** Draft — awaiting confirmation (do not plan/implement until approved)
**Vision source:** `RUSTGO.md` + PDF (identical)
**Resume lens:** `resume.md` — Licence pro, projects-as-experience, ASO bullets, tailoring
**Goal framing:** Validated research/portfolio build — not production-intent

---

## 1. Summary

Build a polyglot financial platform — **Go API Gateway** (JWT-authenticated edge; full Zero Trust is a later slice) + **Rust Reconciliation Engine** (saga/integrity core) — communicating via a hybrid stack: **shared-memory ring for the hot path (experimental/benchmark path) + gRPC for orchestration** (`RUSTGO.md:36`). The first deliverable is a **vertical tracer bullet**: the thinnest end-to-end path that proves the two languages talk correctly and a saga can be tracked. The **resume deliverable** is a later showcase slice that ingests a simulated MVola/Orange Money settlement file, auto-matches it, and survives a crash on demo — the artifact a bank/operator hiring manager in Madagascar actually cares about.

> **Scope disclaimer:** This is an **experimental prototype / reference architecture / simulation tool**, not a production financial system. It has not been independently audited for security, compliance, or correctness and must not handle real money.

Pursued slices are independent specs → plans → builds. This doc covers Slice #1 in full and Slices #2–4 as roadmap with acceptance criteria.

---

## 2. Goals

- Prove the polyglot contract early: Go writes → Rust reads via POSIX shared memory on WSL2/Linux (Windows remains editor-only). Shm is the benchmark path; prod path is file/queue adapters (§9).
- Establish the saga lifecycle + compensating transaction foundation and a WAL-backed recovery story (`RUSTGO.md:54` RTO/RPO), with **mandatory idempotency** from day one.
- Instrument the path so `RUSTGO.md:50` metrics (p99 <10ms @10k RPS, >50k RPS 24h, >99.9% match rate) can be measured without re-architecture — no metric claimed until recorded.
- Produce a credible enterprise integration story and a demoable showcase — not a benchmark toy.

## 3. Non-Goals (for Slice #1)

- Full Zero Trust (PDP/PIP split, OIDC/JWKS, mTLS, WAF, device identity) — slice #1 is "JWT-authenticated gateway with a simple policy prototype"; do not market as Zero Trust.
- Matching/rules engine, external-bank APIs, automated exception workflow — slice #3.
- Recorded benchmark numbers — instrumentation only in slice #1; numbers in slice #4.
- Replacing a bank's existing gateway/core — integration is sidecar/proxy + file/DB adapters (see §9).
- Production-grade source of truth — `HashMap + WAL` in slice #1 is **not** a queryable source of truth; Postgres in slice #2 is required before that claim.

---

## 4. Decisions Already Locked

| Decision | Choice | Why |
| :--- | :--- | --- |
| Primary goal | Validated research/portfolio | `RUSTGO.md:42` frames benchmarks as north star |
| Runtime target | WSL2/Linux | `shmipc` is POSIX-first; no silent Windows assumption (`AGENTS.md`) |
| Build sequence | Vertical tracer bullet | Earliest integration risk, earliest demoable path |
| Tracer auth | JWT HS256 + single policy, **mandatory Idempotency-Key** | Proves PEP before PDP/PIP; OIDC later; idempotency is not optional for financial transfers |
| Saga state | In-mem HashMap + WAL (append-only file) | Zero external deps, exercises RTO/RPO directly; Postgres added slice #2 |

---

## 5. Architecture

### 5.1 Layering (`RUSTGO.md:40` layered model)

```
Client  →  Go Gateway (JWT-auth prototype, net/http, :8080)  ──shm ring (bench)──►  Rust Engine (saga table + WAL)
              │  gRPC client ◄────────────────────────────── gRPC server (tonic) ─┤
              │  → audit log, → proxies to legacy services                        │
              └───────────────────────────────────────────────────────────────────┘
                         ▲ settlement CSV (SFTP/file) arrives slice #3 — production path
```

- Gateway is ingress: validates JWT + Idempotency-Key, authorizes (single rule), audits, routes, emits events. Failure of the engine does not corrupt gateway state — but backpressure is honest: ring-full → `503` after ~100s @10k RPS (see §6, §8).
- Engine is co-processor: consumes events, owns saga state, WALs every transition (fsync per record — see throughput trade-off §8), serves status. Fault-tolerant by design; decoupled scaling. `HashMap + WAL` is **not** a production source of truth — Postgres in slice #2 is.
- Control plane is gRPC (`RUSTGO.md:38`) — typed contracts, later bidirectional streaming for reports.

### 5.2 Repo Layout (scaffolded when a plan executes)

```
gateway/                 Go module secureledger/gateway
  cmd/gateway/           main: HTTP server, proxy wiring
  internal/authn/        JWT HS256 validation + Idempotency-Key enforcement (stdlib + golang-jwt)
  internal/policy/       single rule: subject may only move own accounts
  internal/ipc/          ring writer (syscall.Mmap, /dev/shm/secureledger.ring) — bench path
  internal/audit/        structured audit log line per request (append-only, not yet tamper-proof)
engine/                  Rust crate
  src/ring.rs            ring reader (memmap2)
  src/saga.rs            saga state machine + WAL append/replay
  src/grpc.rs            tonic server: GetSagaStatus
  src/main.rs            startup: replay WAL → spawn ring consumer → serve gRPC
proto/
  ledger.proto           GetSagaStatus, future streaming
  ipc-wire.md            THE contract between languages — versioned, canonical
docs/superpowers/specs/  this doc
```

---

## 6. Wire Contract — The Real Interface

Fixed **256-byte slots**, SPSC, two cache-line-padded `u64` counters (`head` writer-only, `tail` reader-only), monotonic indices masked by ring size. No variable-length framing across languages.

```
Slot (256B):
  type: u8            // SAGA_START, STEP_OK, STEP_FAIL, COMPENSATE
  flags: u8
  reserved: u16
  saga_id: [u8;16]    // UUID v4
  timestamp_ns: u64
  payload_len: u16    // 0..224
  payload: [u8;224]   // JSON, truncated to 224B in slice #1
  // remainder zero-padded
```

- Ring size for tracer: **1M slots × 256B = 256 MB** in `/dev/shm` — **~100 seconds headroom @10k RPS** (`1_000_000 / 10_000 = 100s`). For reference, 24h @10k RPS = 864M events ≈ 221 GB. The ring is therefore a **short backpressure buffer / benchmark path, not durable storage**. It does not make `RUSTGO.md:40` "engine failure doesn't block gateway" true under sustained outage — ring-full → honest `503` (see §8). Future slices may tune size or spill to disk/queue; this spec does not claim 24h headroom.
- `proto/ipc-wire.md` is normative: magic, version, slot layout, memory ordering (Release/Acquire on head/tail), and a hex fixture.
- `ponytail:` ceiling — 224B payload wastes bytes. Add continuation slots when a real event needs >224B; not before.
- **Disclaimer:** The shm ring is an **experimental/reference/benchmark path** for co-located Go+Rust on WSL2/Linux. Recommended production transport is Postgres outbox / Kafka / NATS / SFTP adapters (see §9).

---

## 7. Slice #1 Behavior (Tracer)

### Gateway — JWT-authenticated prototype (not full Zero Trust)

- `POST /transfers` — requires `Authorization: Bearer <JWT>` (HS256 in slice #1; RS256/JWKS in slice #2) **and** `Idempotency-Key: <uuid>` header. Validates signature + expiry + `Idempotency-Key` presence **before** any ring write; nothing unauthenticated or non-idempotent ever reaches the buffer (`RUSTGO.md:11` trust-minimized stream). Missing/duplicate `Idempotency-Key` → `400`/`409` without ring write. Duplicate key returns the original `saga_id` (deduplication via in-mem map in slice #1, Postgres unique index in slice #2).
- Policy: `subject == body.from_account` else `403`. One rule intentionally. **Not full Zero Trust** — PDP/PIP split, device identity, JWKS rotation, mTLS arrive in slice #2. Slice #1 is "JWT-authenticated gateway with a simple policy enforcement prototype."
- On success: generates `saga_id`, writes `SAGA_START{from,to,amount,idempotency_key}` to ring, appends audit line `{ts, subject, route, decision, saga_id, idempotency_key, latency_ms}`, returns `202 {saga_id, status:"accepted"}`.
- `GET /sagas/:id` — proxies to engine gRPC `GetSagaStatus`.

### Engine

- On start: replay WAL file (`data/wal.log` append-only, `fsync` per record — see §8 throughput note) into `HashMap<SagaId, Saga>`. Duplicate `idempotency_key` → return existing `saga_id` without creating a new saga.
- Ring consumer: parse slot → validate `payload_len` → deserialize JSON → validate `idempotency_key` → apply transition:
  - `SAGA_START` (new key) → `InProgress` + WAL
  - `SAGA_START` (duplicate key) → no state change, emit `DUPLICATE` metric
  - `STEP_FAIL` (injected by test client in slice #1) → `Compensating` → `Compensated` + WAL
- gRPC `GetSagaStatus(saga_id) → {state, steps[], updated_at}`; unknown → `NOT_FOUND`.

### Flow

```
Client POST /transfers (JWT + Idempotency-Key) → gateway policy + dedup OK → ring SAGA_START → 202
  → engine consumes → InProgress (WAL, fsync)
  → retry with same Idempotency-Key → 409/200 with original saga_id, no new saga
  → test client POST /transfers/:id/fail → ring STEP_FAIL
  → engine → Compensated (WAL)
  → client GET /sagas/:id → gRPC → {Compensated}
```

---

## 8. Error Handling & Fault Tolerance

**Trust boundary** — `401` bad/expired JWT (including `exp`/`nbf` checks), `400` missing/invalid `Idempotency-Key`, `409` duplicate key (returns original saga), `403` policy violation, `400` malformed body. All rejected before ring write.

**Backpressure (refines vision)** — `RUSTGO.md:40` claims engine failure shouldn't impact gateway. Honest refinement: ring-full → bounded spin (~100µs) → `503 Service Unavailable` + `ring_full_total` counter. At 1M×256B, this fires after **~100s @10k RPS**, not hours — 24h would need ~221 GB. Silently dropping financial events would violate integrity. Spec states this deviation explicitly.

**Duplicate/replay protection** — `Idempotency-Key` is the dedup key. Gateway rejects duplicate keys without writing to the ring; engine double-checks on consume. No idempotency key → no saga. This is mandatory for financial correctness (`RUSTGO.md:10`).

**Engine corruption** — bad `payload_len` / truncated JSON → skip slot, increment `corrupt_slots_total`, keep consuming. One bad event must not stall the ledger.

**WAL durability vs throughput (trade-off made explicit)** — Slice #1 uses **fsync per record** (strong durability, RPO = last record, seconds) at the cost of throughput/latency. This **conflicts** with `RUSTGO.md:50` targets of <10ms p99 @10k RPS and >50k RPS sustained — fsync per event is a known bottleneck. Slice #4 benchmarks must prove whether per-record fsync sustains 10k RPS on target hardware. Alternative for later slices: **group/batch commit** (higher throughput, RPO = last flushed batch). Spec does not claim both simultaneously without measurement.
**WAL failure** — if `fsync` fails, halt consumption and exit non-zero. Better down than inventing state. Recovery = replay on restart; RPO = last fsynced record (seconds). Corrupted WAL tail → truncate to last valid record, count `wal_corrupt_tail_total`, require operator review before restart.

**Crash demo** — `kill -9` engine → restart → WAL replay → `GetSagaStatus` returns correct state. This is the interview money shot for RTO/RPO.

---

## 9. Enterprise Integration Story (Why This Is Not a Toy)

A Malagasy enterprise (Telma, MVola merchant, BNI/BOA) will not deploy a student gateway. Integration is **incremental, sidecar-style**, and the repo must say so:

- **Gateway as sidecar proxy** — DNS → Go gateway → existing payment/account services. Terminates OIDC (JWKS, later slice), adds `X-Subject`/`X-Saga-Id`, enforces rate limits. Starts in **shadow/audit mode** (log decisions, don't block), then enforce. Zero code change in legacy. Secrets via env/file, never committed; key rotation is a documented limitation in slice #1.
- **Engine via adapters, not shm in prod** — shm is the **WSL2 co-location benchmark path only** (see §6 disclaimer). Prod integration uses adapters added slice #3: **SFTP/CSV settlement file poller** (MVola/Orange Money format), Postgres outbox poller/CDC, and `POST /reverse` compensating calls with `Idempotency-Key` (`RUSTGO.md:10` — critical).
- **Credibility checklist** (slices #2–3): Postgres as queryable source-of-truth behind WAL (slice #1 HashMap is **not** that), OpenTelemetry traces + Prometheus metrics + structured audit log, configurable matching rules (exact + tolerance/fuzzy, fee/timestamp handling — specified in slice #3), exception queue/dashboard.

If we ship only shm + in-mem state, it's a benchmark toy. With SFTP/CSV + Postgres + shadow proxy + `docs/integration.md`, it's a story an IT director recognizes.

**Open-source usability minimum** (required before claiming "usable"): `README.md` with 5-minute `docker compose up && make demo`, `LICENSE`, `SECURITY.md`, `CONTRIBUTING.md`, `docs/architecture.md`, `docs/integration.md`, `docs/limitations.md`, `sample-data/` (clean + duplicate/missing/amount-mismatch/fee-mismatch/corrupted rows), `Makefile`, CI.

---

## 10. Verification

### Slice #1 (smallest checks that fail if broken)

- **Go:** ring writer unit test (write N events → read raw bytes back); JWT/policy table tests including `Idempotency-Key` deduplication; audit log format test.
- **Rust:** ring reader parse against `ipc-wire.md` hex fixture; WAL round-trip (write → replay → equal map) including corrupted-tail truncation; saga transition tests including duplicate-key handling.
- **Cross-language (WSL2 only):** `scripts/tracer.sh` — boots gateway + engine, fires 100 transfers (including 10 retries with same `Idempotency-Key`), injects `STEP_FAIL`, asserts every `GetSagaStatus` matches expected lifecycle and duplicates created no new saga. This is also the proto/wire contract check.
- **Static:** `go vet ./...`, `go test ./...`, `cargo clippy -- -D warnings`, `cargo test`.

### Later (Slices #2–4)

- `go vet` + `cargo clippy` remain mandatory per `AGENTS.md`.
- Load test (slice #4): recorded `p50/p95/p99` at 10k RPS and sustained 24h run for `RUSTGO.md:51` targets — graphs committed, not estimates. Match-rate report from seeded settlement mismatches.

---

## 11. Instrumentation (Slice #1 Hooks, Numbers Later)

- Gateway: histogram around ring write; counters `ring_full_total`, `duplicate_key_total`, `unauthorized_total`; audit log latency.
- Engine: histogram ring consume → WAL fsync (`wal_fsync_latency_ms` — will reveal per-record cost); counters `corrupt_slots_total`, `wal_corrupt_tail_total`, `duplicate_events_total`; gauge `wal_replay_duration_ms` on startup.
- No benchmark claims in slice #1 — hooks exist so baselines are one flag away. No `p99`/`RPS`/`>`99.9% claimed until recorded on defined hardware + dataset.

---

## 12. Resume Mapping (`resume.md` strategy)

**Section placement:** Projects above Education (`resume.md:22`). This platform is the lead project.

**Title line** (`resume.md:56` — domain + complexity). Do not claim "Zero-Trust" or "production/bank-grade" until defensible:

```
Secure Settlement Reconciliation Prototype — JWT-Authenticated Gateway (Go)
+ Reconciliation Engine (Rust) for Mobile-Money-Heavy Markets
Go, Rust, gRPC/Protobuf, POSIX shm (bench path), Postgres, JWT | github.com/you/secure-ledger
```

**Three ASO bullets** (`resume.md:82` — Action, Scale, Outcome). Honest if slice #1 only; stronger after slice #3. Numbers are targets until slice #4 records them — never claim `p99`/`>`99.9% without measurement:

- *Engineered Go→Rust shared-memory tracer with fixed 256B SPSC slots (syscall.Mmap + memmap2) on WSL2/Linux — experimental benchmark path; validated slot layout + saga lifecycle via cross-language fixture tests.*
- *Implemented saga state machine with mandatory Idempotency-Key deduplication and WAL-backed recovery (fsync per record, replay on restart; Postgres planned as source-of-truth) — restoring correct state after kill-9 in automated tests.*
- *(After slice #3) Built simulated settlement CSV ingestion (MVola/Orange Money format) matching gateway transactions against external file; auto-matching measured on defined 10k-row dataset with seeded discrepancies, exceptions routed to review queue.*

If only slice #1 is done, use the first two bullets; add the third only after slice #3 is measured. See §14 for honest fallback bullets.

**Skills section** (`resume.md:110`): categorize; only list what bullets prove. Rust is justified in one clause — "predictable tail latency for batch reconciliation" — not decoration.

**Tailoring** (`resume.md:98`): per posting, mirror their exact keywords — *reconciliation/rapprochement/settlement* for operators, *Zero Trust/sécurisation API* for banks.

---

## 13. Roadmap (each slice = its own spec → plan → build)

| Slice | Focus | Serves Metric | Resume Value |
| :--- | :--- | :--- | :--- |
| **#1 Tracer** | Ring + JWT + saga + WAL + gRPC status (this spec) | Instrumentation for p99/RTO | "Can build polyglot correctly" |
| **#2 Hardening** | Postgres as source-of-truth (WAL stays durability path), JWKS/OIDC, mTLS to backends, hardened audit/secret handling | Durability, auditability | Answers "where's the DB?" |
| **#3 Showcase** | Settlement CSV/SFTP adapter, matching/rules engine (exact + tolerance/fuzzy, fee/timestamp), exception workflow, minimal dashboard, sample data | >99.9% match rate (measured on defined dataset) | Star demo for Madagascar |
| **#4 Validation** | Recorded load tests (p50/p95/p99 @10k RPS, sustained run), match-rate report, crash-recovery demo, README + `docs/limitations.md` + `SECURITY.md` | All `RUSTGO.md:50` targets — only if measured | Linkable, interview-ready |

---

## 14. Acceptance Criteria

**Slice #1 done when:**

- [ ] `POST /transfers` with valid JWT + `Idempotency-Key` returns `202` and `GET /sagas/:id` returns lifecycle via gRPC; invalid JWT/policy/missing key returns `401`/`403`/`400` without touching the ring.
- [ ] Duplicate `Idempotency-Key` returns original `saga_id` and creates no new saga (gateway + engine dedup).
- [ ] 100 transfers (incl. 10 duplicate-key retries) with injected failures all reach `Compensated`; duplicates did not inflate saga count.
- [ ] `kill -9 engine; restart` replays WAL and returns correct statuses (RPO = last fsynced record, seconds); corrupted WAL tail truncates safely.
- [ ] `scripts/tracer.sh` passes on WSL2/Linux; `go vet` + `cargo clippy -- -D warnings` clean — no `p99`/`RPS`/`match rate` claimed without recorded run.
- [ ] Audit log line per request exists; counters `ring_full_total`, `corrupt_slots_total`, `duplicate_key_total` exist.
- [ ] `proto/ipc-wire.md` fixture matches both implementations byte-for-byte.
- [ ] `README.md` states: "experimental prototype, not for production financial transactions, not independently audited" and `docs/limitations.md` lists ring headroom (~100s @10k RPS), per-record fsync cost, and non-Zero-Trust scope.

**Showcase done when (slices #3–4):** settlement CSV ingested, >99.9% auto-match on seeded data, exception report generated, load-test graphs committed, Docker Compose one-command demo works.

---

## 15. Self-Review (post strict review fixes)

- Ring math corrected: 1M×256B = 256 MB = ~100s @10k RPS; 24h ≈ 221 GB — not 27h. §6, §8, §11 updated.
- fsync per record vs throughput trade-off made explicit (§8, §11); no dual claim without benchmark.
- Idempotency made mandatory (§4, §7, §8, §10, §14); duplicate-key behavior specified end-to-end.
- "Zero Trust" downgraded to "JWT-authenticated prototype" in §1, §3, §7, §12; full Zero Trust deferred to slice #2.
- shm disclaimer added (§1, §6, §9): experimental/benchmark path, not production transport.
- Source-of-truth clarified: HashMap+WAL is not that; Postgres in slice #2 is (§3, §5, §9, §13).
- No TBD/TODO; `cont.` slots deferred with `ponytail:` ceiling noted.
- No contradiction: ring + 503 refinement reconciles with `RUSTGO.md:40` fault-tolerance claim with correct math.
- Scope is one vertical slice + bounded roadmap — not a monolithic build that risks unfinished repo.
- WSL2/Linux target respected throughout.
- Resume claims gated: no `p99`/`>`99.9%/`RTO` without recorded measurement; honest fallback bullets in §12.

---

**Awaiting your confirmation before writing a plan.** Reply `approved` or request changes.
