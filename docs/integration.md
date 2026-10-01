# Integration — why this is not a toy

A Malagasy enterprise (operator, merchant aggregator, bank) does not deploy a
student gateway. Integration is incremental and sidecar-style:

## Gateway as sidecar proxy

DNS → Go gateway → existing payment/account services. Terminates auth,
adds `X-Subject`/`X-Saga-Id` style headers (saga_id in payload today),
enforces per-sender velocity and amount policy. Starts in **shadow/audit
mode** (log decisions, never block — every decision already has an audit
line), then enforce. Zero code change in legacy services. Secrets via
env/file, never committed; HS256→OIDC rotation is a documented limitation.

## Engine via adapters, not shm in prod

The shm ring is the WSL2 co-location benchmark path only. The file transport
demonstrates the production pattern: an **outbox tail** (Postgres outbox
poller/CDC in real deployment — same offset-replay semantics as
`tail.rs`). Settlement arrives as it does in reality: **SFTP/CSV drops**
(`SETTLEMENT_DIR` is the demo of that poller), matched against intake with
exact + fee/timestamp tolerance, exceptions to a review queue (the report's
missing/orphan/mismatch lists).

## Credibility checklist (shipped)

- [x] Postgres as queryable source-of-truth behind the WAL (degraded-safe)
- [x] Structured audit log + Prometheus-style counters (`/metricsz`)
- [x] Configurable tolerance (fee window, timestamp skew as constants)
- [x] Exception queue shape (report lists + review-hold path)
- [ ] OIDC/JWKS, mTLS to backends, `POST /reverse` compensating calls —
      named next slices, not silent gaps (see limitations)
