# Spec: SettleLedger Showcase — Windows-Demoable Settlement Reconciliation

**Date:** 2026-10-01
**Status:** Draft — awaiting user review (no implementation until approved)
**Vision source:** `RUSTGO.md` + tracer spec `docs/superpowers/specs/2026-08-22-tracer-bullet-design.md`
**AIR link:** Project B core (short-lived certs → JWT now, SAN authz, audit log) +
Project D core (append-only log, deterministic replay, AP-explicit) +
Project A workload (velocity/threshold/geo rules as gateway plugin)
**Goal framing:** Valuable open-source enterprise tool + visual demo + recorded numbers

---

## 1. Summary

The tracer bullet proved Go→Rust over a WSL2-only shm ring. This spec turns it
into something demonstrable and useful: a **cross-operator mobile-money
settlement reconciliation showcase** (MVola/Orange/Airtel simulated) that runs
**on Windows**, shows live settlement in a dashboard, survives chaos on camera,
and reports recorded stress numbers. Shm ring stays untouched as the WSL2 bench
path; a file/TCP JSONL transport carries the Windows demo (and doubles as the
production-plausible outbox pattern).

> **Scope disclaimer:** experimental prototype / reference architecture /
> simulation tool. Not audited, never handles real money (inherits tracer
> `docs/limitations.md`, extended in §9).

## 2. Goals

- 5-minute visual demo on Windows: two binaries → dashboard → 10k seeded
  transfers → live SETTLED/DISCREPANCY states → chaos → recovery.
- Settlement matching that means something: exact + fee/timestamp tolerance
  over a seeded CSV with known discrepancies; measured match-rate.
- Postgres as queryable source-of-truth behind the WAL (tracer HashMap is not).
- Recorded stress: p50/p95/p99 + sustained RPS + replay timer + match-rate
  report, committed as logs. Serves `RUSTGO.md:50` (p99 <10ms @10k RPS,
  >99.9% match) — targets until measured, never claims.
- OSS-usable: LICENSE, SECURITY.md, CONTRIBUTING.md, sample-data, compose,
  CI green on Windows.

## 3. Non-Goals

- Full Zero Trust (OIDC/JWKS, mTLS, SPIRE, WAF) — stays JWT HS256 + one rule;
  `ponytail:` add when an enterprise deployment (not a demo) demands it.
- ML fraud scoring (Isolation Forest) — rules v1 only, per AIR A-v1 bar.
- Shm ring changes — frozen contract (`proto/ipc-wire.md` untouched).
- 24h sustained run — short sustained window (10 min) + replay timer; 24h is
  a later slice with hardware to match.

## 4. Decisions Locked

| Decision | Choice | Why |
| :--- | :--- | --- |
| Demo transport | File/TCP JSONL (`data/events.log`, tail + offset) | Windows-native; same JSON schema as ring payload; offset = replay position (redb-position analogue) |
| Bench transport | Existing shm ring, unchanged | Don't break the proven path; WSL2 numbers later |
| Transport select | `TRANSPORT=file\|shm` env, gateway + engine | One flag, no code fork |
| Matching core | Exact (txid+amount) → tolerance (fee window, ts skew) → DISCREPANCY | DFA-compatible; tolerance recorded in `steps` |
| Source of truth | Postgres (compose for demo) behind WAL | WAL stays durability path; PG is queryable |
| Dashboard | Gateway static HTML + SSE, stdlib only | Zero new deps; gateway already fronts engine via gRPC |
| Chaos | Partition toggle (stop tail/replay) + kill-9 drill | AIR D "channel stop/replay"; interview money shot |
| Fraud plugin | Velocity, threshold, geo-impossibility → review queue | Stateless v1, in-mem windows; PG later |

## 5. Architecture

```
Seeded clients (MVola/Orange/Airtel CSV + live POST)
  → Go Gateway :8080 (JWT, Idempotency-Key, policy, A-rules, audit)
      → TRANSPORT=file: data/events.log ──tail──► Rust Engine
      → TRANSPORT=shm:  /dev/shm ring ──mmap──► (unchanged bench path)
  → Rust Engine (replay DFA, CSV matcher, WAL→Postgres, gRPC :50051)
  → Gateway /dashboard (SSE) + /settlement/upload → engine
  → Postgres (sagas, matches, audit mirror) + Prometheus counters
```

- Gateway never blocks on engine for intake (file append is the buffer;
  honest `503` only when disk/queue backpressure trips — same honesty rule).
- Engine owns all settlement truth; gateway owns edge decisions + presentation.

## 6. Behavior

**Gateway additions:** `TRANSPORT` select; file writer (append JSONL
`{type,saga_id,ts,payload}`, fsync per batch); `POST /settlement/upload`
(proxies CSV to engine); `GET /dashboard` (static); `GET /events/stream`
(SSE: intake, decisions, saga states, discrepancies, metrics); A-rules plugin
(velocity >5 tx/min/sender; amount threshold table; geo <30s cross-operator →
`fraud_hold` to review queue, never silent drop).

**Engine additions:** file-tail consumer with persisted offset (replay from
offset = deterministic replay); CSV settlement ingest; matcher
(exact → tolerance → DISCREPANCY classes: `missing` / `orphan` / `mismatch`);
partition mode (pause tail, keep intake; resume = replay catch-up);
Postgres sink (sagas, matches, audit mirror); counters
(`matched_total{mode}`, `discrepancy_total{class}`, `replay_duration_ms`).

**Dashboard (one page):** live flow (intake → states), discrepancy queue
table, chaos buttons (partition on/off, recovery checklist), metrics panel
(RPS, p99, match-rate — rendered from last committed bench report, labeled
with date/hardware or "not yet measured").

## 7. Error Handling & Fault Tolerance

Trust boundary unchanged (401/400/409/403 before any write). New: malformed
CSV row → skip + `csv_corrupt_total`, keep ingesting; tolerance-window
expiry → `missing`; engine kill-9 → WAL+offset replay → identical states
(drill script asserts equality); PG down → engine keeps WAL/file truth,
marks `pg_degraded`, backfills on reconnect (AP-explicit, per D core).

## 8. Enterprise Integration Story

Sidecar proxy (shadow/audit mode first, enforce later) + SFTP/CSV poller for
real operator files + Postgres outbox poller shape (file tail is the demo of
that pattern) + `POST /reverse` compensating shape. `docs/integration.md`
says this; `docs/limitations.md` gains: file-transport throughput ceiling
(measured in §10, not asserted), HS256 + one rule, rules-v1 false-positive
stance, 10-min sustained (not 24h).

## 9. Verification

- Unit (extend tracer): matcher table tests (exact/tolerance/classes);
  A-rules tests; tail-offset replay test; SSE handler test. Still:
  `go vet/test`, `cargo clippy/test`.
- Golden CSV (`sample-data/`): clean 10k + seeded dup/missing/mismatch/fee/
  corrupt rows with expected report checked byte-for-byte by
  `scripts/showcase-check` (Windows-runnable).
- Recovery drill: kill-9 engine → restart → states identical (script-asserted).
- Cross-transport parity: same seed via `shm` (WSL2) and `file` → same final
  states (contract check beyond the wire fixture).
- Static: Windows CI (go test + cargo test + clippy), `grep TODO/TBD` empty.

## 10. Instrumentation & Stress (numbers, not estimates)

`scripts/stress` Go loader (stdlib, N workers, `TRANSPORT=file`): p50/p95/p99
@10k RPS target, 10-min sustained, results → `bench/results/<date>/`
(JSON + summary md + hardware note). Engine replay timer: 1M seeded events
consume time. Match-rate report from golden CSV. Dashboard reads the latest
report; "not yet measured" otherwise. No `RUSTGO.md:50` claim until a report
exists.

## 11. Roadmap (each phase = Oracle gate)

| Phase | Focus | Gate question |
| :--- | :--- | :--- |
| **#1 Land tracer** | Merge `agent/tracer-bullet` → main; Windows CI; baseline logs | Tracer sound on Windows CI? |
| **#2 Showcase** | File transport, CSV+matcher, PG sink, A-rules, dashboard, golden CSV | Matching correct, demo reproducible in 5 min? |
| **#3 Proof** | Stress logs, recovery drill log, OSS polish (LICENSE/SECURITY/ CONTRIBUTING/sample-data/compose), README demo | Numbers honest, repo linkable? |

## 12. AIR / Resume Mapping

Serves AIR B-min (edge decisions + audit), D (replay/AP-explicit/partition),
A-v1 (rules workload). Interview sentence: "Cross-operator settlement was
manual and partition-fragile; I built intake-to-replay with measured
match-rate and a kill-9 drill — AP with explicit reconciliation."
Honest bullets: tracer (shipped, green) → file-transport + matcher (this
spec) → numbers only after `bench/results` lands.

## 13. Acceptance

- [ ] `TRANSPORT=file` demo runs on Windows: two binaries + PG compose.
- [ ] 10k seeded transfers → dashboard shows live states; golden CSV report
      matches expected byte-for-byte.
- [ ] Partition on/off → catch-up replay; kill-9 → identical states.
- [ ] `bench/results/<date>/` committed with p50/p95/p99, sustained RPS,
      replay time, match-rate + hardware note.
- [ ] CI green on Windows; README 5-min demo works from clone.
- [ ] No "Zero Trust/production/bank-grade" language; limitations extended.

## 14. Self-Review

- No TBD/TODO; every behavior has an owner (gateway vs engine) and a test.
- Shm frozen — no contract risk to the proven path.
- fsync/batch trade-off restated for file transport (§7–8); PG-down stance
  is AP-explicit, consistent with D core.
- Metrics gated: "not yet measured" default in dashboard (§6, §10).
- Scope is one showcase slice in 3 gated phases — no monolith.
- `ponytail:` OIDC/mTLS/ML/24h cut with named triggers, not silent drops.
