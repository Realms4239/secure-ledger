# Bench 2026-10-01 — first recorded run

All numbers measured, not estimated. Method: `scripts/showcase.ps1`
(loader `-c 50`, seed defaults: 10000 unique + 100 dup retries).

## Loader (gateway edge, TRANSPORT=file)

From `loader.json`: 10100 posts, 10000 accepted, 100 duplicate (409),
0 held, 0 transport errors.

- wall 38.87s → **259.8 RPS**
- p50 180.6ms, p95 361.8ms, **p99 494.8ms**

## Matcher (engine)

From `match-report.json`: settled=9805, tolerated=150, missing=20,
orphan=15, mismatch=25, csv_corrupt=10 — exact vs `expected-report.json`.

## Hardware + conditions (read before comparing)

- Intel i5-5200U (2 cores) @ 2.20GHz, 8GB RAM, Windows 10 Pro
- Go 1.26.5 / Rust 1.98.0 (dev profiles, not release)
- **C: system disk at ~0 bytes free during the run** (external drain, not
  repo data): expect severe pagefile/IO stalls. p99 here measures a
  degraded laptop, not the architecture. Re-run on a healthy disk +
  `--release` before quoting these numbers anywhere.

## vs RUSTGO bar

- p99 <10ms @10k RPS: NOT MET (this run: p99 495ms @ 260 RPS, degraded disk)
- match rate: every seeded class resolved exactly as seeded (9805 settled,
  150 tolerated, 25 mismatch, 15 orphan, 20 missing, 10 corrupt counted) —
  100% class accuracy on the golden set; RUSTGO's >99.9% bar refers to
  production traffic, not yet measured
- sustained 24h / 50k RPS: not attempted (later slice, proper hardware)
