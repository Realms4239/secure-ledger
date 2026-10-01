# Recovery drill: snapshot the match report, kill -9 the engine, restart it,
# and prove WAL replay restores identical state.
# Usage: powershell -ExecutionPolicy Bypass -File scripts/recovery.ps1
$ErrorActionPreference = "Stop"

$Root = Split-Path -Parent $PSScriptRoot
$Secret = "recovery-secret-32-bytes-long!!"
$GwAddr = "127.0.0.1:8081"
$EngAddr = "127.0.0.1:50052"
# D: run root — C: is routinely full on dev machines.
$Tmp = Join-Path "D:/showcase-runs" ("recovery-" + $PID)
$Data = Join-Path $Tmp "data"
$SeedDir = Join-Path $Tmp "seed"
New-Item -ItemType Directory -Force -Path $Data, $SeedDir, (Join-Path $Data "settlement") | Out-Null

$env:GOCACHE = "D:/go-cache"
$env:GOTMPDIR = "D:/go-tmp"
$env:CARGO_TARGET_DIR = "D:/sl-target/settleledger"

function Fail($msg) { Write-Host "RECOVERY FAIL: $msg"; exit 1 }

Push-Location (Join-Path $Root "gateway")
go build -o (Join-Path $Tmp "gateway.exe") ./cmd/gateway
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "gateway build" }
go build -o (Join-Path $Tmp "loader.exe") ./cmd/loader
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "loader build" }
go run ./cmd/seed -n 500 -dups 5 -dir $SeedDir
if ($LASTEXITCODE -ne 0) { Pop-Location; Fail "seed" }
Pop-Location

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
  RATE_LIMIT_RPS = "2000"; RATE_LIMIT_BURST = "4000"
}
$EngineBin = "D:/sl-target/settleledger/debug/secureledger-engine.exe"
$eng = $null; $gw = $null
function Start-Engine($logSuffix) {
  foreach ($k in $engEnv.Keys) { Set-Item "env:$k" $engEnv[$k] }
  return (Start-Process -FilePath $EngineBin -PassThru `
    -RedirectStandardOutput (Join-Path $Tmp "engine$logSuffix.log") `
    -RedirectStandardError (Join-Path $Tmp "engine$logSuffix.err"))
}
try {
  $eng = Start-Engine ""
  foreach ($k in $gwEnv.Keys) { Set-Item "env:$k" $gwEnv[$k] }
  $gw = Start-Process -FilePath (Join-Path $Tmp "gateway.exe") -PassThru `
    -RedirectStandardOutput (Join-Path $Tmp "gateway.log") `
    -RedirectStandardError (Join-Path $Tmp "gateway.err")

  $ok = $false
  for ($i = 0; $i -lt 30; $i++) {
    try { if ((Invoke-WebRequest "http://$GwAddr/healthz" -UseBasicParsing).StatusCode -eq 200) { $ok = $true; break } } catch {}
    Start-Sleep 1
  }
  if (-not $ok) { Fail "gateway never healthy" }

  & (Join-Path $Tmp "loader.exe") -url "http://$GwAddr" -jwt-secret $Secret -transfers (Join-Path $SeedDir "transfers.jsonl") -c 10 | Out-Null
  if ($LASTEXITCODE -ne 0) { Fail "loader" }
  $tok = & (Join-Path $Tmp "loader.exe") -jwt-secret $Secret -token "alice"
  $csv = [IO.File]::ReadAllText((Join-Path $SeedDir "settlement-10k.csv"))
  Invoke-RestMethod -Method Post -Uri "http://$GwAddr/settlement/upload" -Headers @{Authorization = "Bearer $tok"} -Body $csv | Out-Null

  $before = $null
  for ($i = 0; $i -lt 60; $i++) {
    Start-Sleep 5
    try { $before = Invoke-RestMethod "http://$GwAddr/report" } catch { continue }
    if ($before.report.settled + $before.report.tolerated -ge 400) { break }
    $before = $null
  }
  if ($null -eq $before) { Fail "report never completed before kill" }
  $beforeJson = $before | ConvertTo-Json -Depth 8 -Compress
  Write-Host ("before kill: settled={0} tolerated={1}" -f $before.report.settled, $before.report.tolerated)

  Write-Host "== kill -9 engine =="
  Stop-Process -Id $eng.Id -Force
  Start-Sleep 2
  if (-not (Get-Process -Id $eng.Id -ErrorAction SilentlyContinue)) {
    Write-Host "engine dead, restarting"
  }
  $eng = Start-Engine "-after"
  Start-Sleep 12  # replay + one sweep cycle

  $after = $null
  for ($i = 0; $i -lt 24; $i++) {
    Start-Sleep 5
    try { $after = Invoke-RestMethod "http://$GwAddr/report" } catch { continue }
    if ($after.report.settled -eq $before.report.settled) { break }
  }
  if ($null -eq $after) { Fail "no report after restart" }
  $afterJson = ($after.report | ConvertTo-Json -Depth 8 -Compress)
  $beforeRepJson = ($before.report | ConvertTo-Json -Depth 8 -Compress)
  Set-Content (Join-Path $Tmp "report-before.json") $beforeRepJson
  Set-Content (Join-Path $Tmp "report-after.json") $afterJson
  $replayLine = Get-Content (Join-Path $Tmp "engine-after.log") | Where-Object { $_ -match "WAL replay done" }
  if ($afterJson -ne $beforeRepJson) { Fail "report differs after replay" }

  $log = "before settled={0} tolerated={1}`nreplay: {2}`nafter: identical report`nRECOVERY PASS" -f `
    $before.report.settled, $before.report.tolerated, $replayLine
  Set-Content (Join-Path $Tmp "recovery.log") $log
  Write-Host $log
  Write-Host "logs kept in $Tmp"
} finally {
  $ids = @()
  if ($eng) { $ids += $eng.Id }
  if ($gw) { $ids += $gw.Id }
  if ($ids.Count -gt 0) { Stop-Process -Id $ids -ErrorAction SilentlyContinue }
}
