# Security Policy

This is an **experimental prototype** — see `docs/limitations.md`. It has not
been independently audited. Do not handle real money or real credentials.

## Reporting a vulnerability

Open a GitHub issue titled `[SECURITY]` with: affected commit, repro steps,
and impact. Do not include secrets or live credentials. Expect triage within
7 days; fixes land as regular commits (no embargo process exists yet).

## Known non-goals (not vulnerabilities)

- JWT HS256 with env-shared secret (no JWKS rotation, no OIDC)
- Postgres `NoTls` in the compose demo (local loopback only)
- Single-policy authorization (`subject == from_account`)
- `POST /chaos/partition` guarded by JWT only — never expose the gateway
  to untrusted networks; it is an edge demo, not a hardened perimeter
