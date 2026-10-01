# Bench 2026-10-01 (hardened gateway)

Second recorded run, same method as `../2026-10-01/` (loader `-c 50`,
seed defaults), after the security-hardening pass: per-IP limiter active
at 2000 RPS / 4000 burst (silent at this load — zero 429s), loopback
binds, body caps, amount bound, fail-route disabled.

- wall 26.68s → **378.6 RPS** (vs 259.8 pre-hardening; delta is disk
  pressure variance, not the limiter — see hardware note)
- p50 112.7ms, p95 286.0ms, **p99 491.2ms**
- counts: 10000 accepted, 100 duplicate, 0 held, 0 errors
- match classes exact vs golden set (9805/150/20/15/25/10)

Hardware: i5-5200U, 8GB, Win10, Go 1.26.5, Rust 1.98.0, dev profiles,
C: disk near-full (same degraded caveat as the first run). RUSTGO bar
(p99 <10ms @10k RPS, 24h) still unclaimed — needs healthy disk + release.
