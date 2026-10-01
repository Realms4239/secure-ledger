# Showcase gate (Windows-native): boots gateway(file)+engine, loads the seed,
# uploads settlement, and byte-compares the match report with expected.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/showcase.ps1
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Secret = "showcase-secret-32-bytes-long!"
$GwAddr = "127.0.0.1:8080"
$EngAddr = "127.0.0.1:50051"
# Run root lives on D: — C: is routinely full on dev machines and Go/cargo
# binaries must link somewhere writable.
$Tmp = Join-Path "D:/showcase-runs" ("showcase-" + $PID)
$Data = Join-Path $Tmp "data"
$SeedDir = Join-Path $Tmp "seed"
New-Item -ItemType Directory -Force -Path $Data, $SeedDir, (Join-Path $Data "settlement") | Out-Null

$env:GOCACHE = "D:/go-cache"
$env:GOTMPDIR = "D:/go-tmp"
$env:CARGO_TARGET_DIR = "D:/sl-target/settleledger"

function Fail($msg) { Write-Host "SHOWCASE FAIL: $msg"; exit 1 }

Write-Host "== build =="
Push-Location (Join-Path $Root "gateway")
go build -o (Join-Path $Tmp "gateway.exe") ./cmd/gateway
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "gateway build" }
go build -o (Join-Path $Tmp "loader.exe") ./cmd/loader
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "loader build" }
go run ./cmd/seed -dir $SeedDir
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "seed" }
Pop-Location
Push-Location $Root
cargo build --manifest-path engine/Cargo.toml
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "engine build" }
Pop-Location
$EngineBin = "D:/sl-target/settleledger/debug/secureledger-engine.exe"

Write-Host "== start engine + gateway (TRANSPORT=file) =="
$engEnv = @{
  TRANSPORT = "file"; WAL_PATH = (Join-Path $Data "wal.log")
  EVENTS_PATH = (Join-Path $Data "events.log"); EVENTS_OFFSET = (Join-Path $Data "events.offset")
  SETTLEMENT_DIR = (Join-Path $Data "settlement"); REPORT_PATH = (Join-Path $Data "match-report.json")
  PARTITION_FLAG = (Join-Path $Data "partition.flag"); ENGINE_ADDR = $EngAddr
}
$gwEnv = @{
  TRANSPORT = "file"; JWT_SECRET = $Secret; ENGINE_ADDR = $EngAddr
  GATEWAY_ADDR = $GwAddr; EVENTS_PATH = (Join-Path $Data "events.log")
  SETTLEMENT_DIR = (Join-Path $Data "settlement"); REPORT_PATH = (Join-Path $Data "match-report.json")
  PARTITION_FLAG = (Join-Path $Data "partition.flag")
}
$engLog = Join-Path $Tmp "engine.log"; $gwLog = Join-Path $Tmp "gateway.log"
$eng = $null; $gw = $null
try {
  foreach ($k in $engEnv.Keys) { Set-Item "env:$k" $engEnv[$k] }
  $eng = Start-Process -FilePath $EngineBin -PassThru -RedirectStandardOutput $engLog -RedirectStandardError (Join-Path $Tmp "engine.err")
  foreach ($k in $gwEnv.Keys) { Set-Item "env:$k" $gwEnv[$k] }
  $gw = Start-Process -FilePath (Join-Path $Tmp "gateway.exe") -PassThru -RedirectStandardOutput $gwLog -RedirectStandardError (Join-Path $Tmp "gateway.err")

  Write-Host "== wait healthy =="
  $ok = $false
  for ($i = 0; $i -lt 30; $i++) {
    try { if ((Invoke-WebRequest "http://$GwAddr/healthz" -UseBasicParsing).StatusCode -eq 200) { $ok = $true; break } } catch {}
    Start-Sleep 1
  }
  if (-not $ok) { Fail "gateway never healthy" }

  Write-Host "== load 10100 transfers =="
  $loaderOut = Join-Path $Tmp "loader.json"
  & (Join-Path $Tmp "loader.exe") -url "http://$GwAddr" -jwt-secret $Secret -transfers (Join-Path $SeedDir "transfers.jsonl") -c 50 -out $loaderOut
  if ($LASTEXITCODE -ne 0) { Fail "loader" }
  $sum = Get-Content $loaderOut -Raw | ConvertFrom-Json
  Write-Host ("accepted={0} dup={1} held={2} other={3} p99={4}ms rps={5}" -f $sum.accepted, $sum.duplicate, $sum.held_for_review, $sum.other, $sum.p99_ms, $sum.rps)
  if ($sum.accepted -ne 10000 -or $sum.duplicate -ne 100 -or $sum.held_for_review -ne 0 -or $sum.other -ne 0) {
    Fail ("loader counts off: " + (Get-Content $loaderOut -Raw))
  }

  Write-Host "== upload settlement =="
  $tok = & (Join-Path $Tmp "loader.exe") -jwt-secret $Secret -token "alice"
  $csv = [IO.File]::ReadAllText((Join-Path $SeedDir "settlement-10k.csv"))
  $up = Invoke-RestMethod -Method Post -Uri "http://$GwAddr/settlement/upload" -Headers @{Authorization = "Bearer $tok"} -Body $csv
  Write-Host ("uploaded " + $up.file)

  Write-Host "== await report =="
  $rep = $null
  # Slow disks need room: engine fsyncs every WAL record (per-record
  # durability is the point), so the tail allows six minutes.
  for ($i = 0; $i -lt 72; $i++) {
    Start-Sleep 5
    try { $rep = Invoke-RestMethod "http://$GwAddr/report" } catch { continue }
    if ($rep.report.settled + $rep.report.tolerated + $rep.report.missing.Count + $rep.report.orphan.Count + $rep.report.mismatch.Count -ge 10000) { break }
    $rep = $null
  }
  if ($null -eq $rep) { Fail "report never completed" }
  $exp = Get-Content (Join-Path $SeedDir "expected-report.json") -Raw | ConvertFrom-Json
  $errs = @()
  if ($rep.report.settled -ne $exp.settled) { $errs += "settled $($rep.report.settled) != $($exp.settled)" }
  if ($rep.report.tolerated -ne $exp.tolerated) { $errs += "tolerated $($rep.report.tolerated) != $($exp.tolerated)" }
  if ($rep.report.missing.Count -ne $exp.missing) { $errs += "missing count $($rep.report.missing.Count) != $($exp.missing)" }
  if ($rep.report.csv_corrupt -ne $exp.csv_corrupt) { $errs += "corrupt $($rep.report.csv_corrupt) != $($exp.csv_corrupt)" }
  $orphanGot = ($rep.report.orphan | Sort-Object) -join ","
  $orphanWant = ($exp.orphan | Sort-Object) -join ","
  if ($orphanGot -ne $orphanWant) { $errs += "orphan set differs" }
  $misGot = ($rep.report.mismatch | Sort-Object) -join ","
  $misWant = ($exp.mismatch | Sort-Object) -join ","
  if ($misGot -ne $misWant) { $errs += "mismatch set differs" }
  if ($errs.Count -gt 0) { Fail ($errs -join "; ") }

  Write-Host ("SHOWCASE PASS: settled={0} tolerated={1} missing={2} orphan={3} mismatch={4} corrupt={5}" -f `
    $rep.report.settled, $rep.report.tolerated, $rep.report.missing.Count, $rep.report.orphan.Count, $rep.report.mismatch.Count, $rep.report.csv_corrupt)
  Write-Host "logs kept in $Tmp"
} finally {
  $ids = @()
  if ($eng) { $ids += $eng.Id }
  if ($gw) { $ids += $gw.Id }
  if ($ids.Count -gt 0) { Stop-Process -Id $ids -ErrorAction SilentlyContinue }
}
