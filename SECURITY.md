# Security Policy

This is an **experimental prototype** — see `docs/limitations.md`. It has not
been independently audited. Do not handle real money or real credentials.

## Reporting a vulnerability

Open a GitHub issue titled `[SECURITY]` with: affected commit, repro steps,
and impact. Do not include secrets or live credentials. Expect triage within
7 days; fixes land as regular commits (no embargo process exists yet).

## What is enforced

- JWT HS256 pinned, expiry required, `sub` required; every route except
  `/healthz`, `/dashboard` and `/bench-latest.json` requires a token
- Per-IP token-bucket rate limiting (default 100 RPS, burst 200;
  `RATE_LIMIT_RPS`/`RATE_LIMIT_BURST`; 429 + `rate_limited_total`)
- Request body caps (1MB JSON; 10MB settlement CSV); integer amounts capped
  at 999,999,999,999 minor units (engine parses i64 — uncapped values
  could 202-accept without ever applying)
- Fail-injection route exists only with `ENABLE_FAIL_INJECT=1`
- Loopback binds by default (`127.0.0.1:8080`, `127.0.0.1:50051`);
  read/idle timeouts on the HTTP server (no write timeout: SSE streams)
- Audit log is JSON-encoded (no log forging); dashboard renders text only

## Known non-goals (not vulnerabilities)

- JWT HS256 with env-shared secret (no JWKS rotation, no OIDC)
- Postgres `NoTls` in the compose demo (local loopback only)
- Single-policy authorization (`subject == from_account`)
- `POST /chaos/partition` guarded by JWT only — never expose the gateway
  to untrusted networks; it is an edge demo, not a hardened perimeter
