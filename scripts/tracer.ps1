#!/usr/bin/env pwsh
# Windows-native tracer smoke test (fallback for scripts/tracer.sh which needs WSL2).
# Boots engine + gateway, fires transfers with duplicate keys + fail injection,
# asserts saga lifecycle via GET /sagas/:id, then kill -9 engine and checks WAL replay.
# WSL2/Linux gate (scripts/tracer.sh) remains the normative contract check.
$ErrorActionPreference = "Stop"
$wt = Split-Path -Parent $MyInvocation.MyCommand.Path | Split-Path -Parent  # repo root
$tmp = Join-Path $env:TEMP "secureledger-tracer-$(Get-Random)"
New-Item -ItemType Directory -Force -Path $tmp | Out-Null
$ring = Join-Path $tmp "secureledger.ring"
$wal = Join-Path $tmp "wal.log"
$secret = "test-secret-32-bytes-long-for-hs256"

Write-Host "== build =="
$eap = $ErrorActionPreference; $ErrorActionPreference = "Continue"  # native tools write stderr noise
Push-Location "$wt\gateway"; cmd /c "go build -o $tmp\gateway.exe ./cmd/gateway 2>&1"; Pop-Location
if ($LASTEXITCODE -ne 0) { throw "gateway build failed" }
$cargo = "$env:USERPROFILE\.cargo\bin\cargo.exe"
$msvc = "C:\Program Files (x86)\Microsoft Visual Studio\2022\BuildTools\VC\Tools\MSVC\14.44.35207"
$sdk = "10.0.26100.0"
$wk = "C:\Program Files (x86)\Windows Kits\10"
$env:Path = "$msvc\bin\Hostx64\x64;$env:USERPROFILE\.cargo\bin;$env:Path"
$env:INCLUDE = "$msvc\include;$wk\Include\$sdk\ucrt;$wk\Include\$sdk\um;$wk\Include\$sdk\shared;$wk\Include\$sdk\winrt"
$env:LIB = "$msvc\lib\x64;$wk\Lib\$sdk\ucrt\x64;$wk\Lib\$sdk\um\x64"
$env:PROTOC = (Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages" -Recurse -Filter "protoc.exe" | Select-Object -First 1).FullName
Push-Location "$wt\engine"; cargo build 2>&1 | Out-Null; Pop-Location
$ErrorActionPreference = $eap
if ($LASTEXITCODE -ne 0) { throw "engine build failed" }
Copy-Item "$wt\engine\target\debug\secureledger-engine.exe" "$tmp\engine.exe"

function Make-JWT([string]$sub) {
    # HS256 JWT via .NET — header.payload.signature base64url
    function b64([byte[]]$b) { [Convert]::ToBase64String($b).TrimEnd("=").Replace("+","-").Replace("/","_") }
    $hdr = b64 ([Text.Encoding]::UTF8.GetBytes('{"alg":"HS256","typ":"JWT"}'))
    $pl = b64 ([Text.Encoding]::UTF8.GetBytes(('{{"sub":"{0}","exp":{1}}}' -f $sub, [DateTimeOffset]::UtcNow.AddHours(1).ToUnixTimeSeconds())))
    $hmac = [System.Security.Cryptography.HMACSHA256]::new([Text.Encoding]::UTF8.GetBytes($secret))
    $sig = b64 ($hmac.ComputeHash([Text.Encoding]::UTF8.GetBytes("$hdr.$pl")))
    return "$hdr.$pl.$sig"
}

try {
    Write-Host "== start gateway (creates ring) + engine =="
    # PS 5.1: children inherit current process env — set before each start
    $env:JWT_SECRET = $secret; $env:ENGINE_ADDR = "localhost:50051"; $env:GATEWAY_ADDR = ":8081"; $env:RING_PATH = $ring
    $gw = Start-Process -FilePath "$tmp\gateway.exe" -PassThru -NoNewWindow
    Start-Sleep -Seconds 2
    if ($gw.HasExited) { throw "gateway exited early" }

    # health check with retry
    $ok = $false
    foreach ($i in 1..10) {
        try { Invoke-RestMethod "http://localhost:8081/healthz" -TimeoutSec 2 | Out-Null; $ok = $true; break } catch { Start-Sleep 1 }
    }
    if (-not $ok) { throw "gateway never became healthy" }
    if (-not (Test-Path $ring)) { throw "ring not created by gateway at $ring" }

    $env:WAL_PATH = $wal; $env:RING_PATH = $ring; $env:ENGINE_ADDR = "127.0.0.1:50051"
    $eng = Start-Process -FilePath "$tmp\engine.exe" -PassThru -NoNewWindow
    Remove-Item Env:WAL_PATH, Env:RING_PATH, Env:ENGINE_ADDR, Env:JWT_SECRET, Env:GATEWAY_ADDR -ErrorAction SilentlyContinue
    Start-Sleep -Seconds 2
    if ($eng.HasExited) { throw "engine exited early" }

    $jwt = Make-JWT "alice"
    $headers = @{ Authorization = "Bearer $jwt" }

    Write-Host "== fire 5 unique + 3 duplicate-key transfers =="
    function Post-Transfer([string]$key, [hashtable]$baseHeaders, [string]$bodyJson) {
        $h = $baseHeaders.Clone(); $h["Idempotency-Key"] = $key
        Invoke-RestMethod -Method Post "http://localhost:8081/transfers" -Headers $h -ContentType "application/json" -Body $bodyJson
    }
    $sagaIds = @()
    for ($i = 1; $i -le 5; $i++) {
        $body = @{ from_account = "alice"; to_account = "bob"; amount = 100 * $i } | ConvertTo-Json
        $r = Post-Transfer ([guid]::NewGuid().ToString()) $headers $body
        $sagaIds += $r.saga_id
    }
    $dupKey = [guid]::NewGuid().ToString()
    $body = @{ from_account = "alice"; to_account = "bob"; amount = 42 } | ConvertTo-Json
    $r1 = Post-Transfer $dupKey $headers $body
    for ($i = 1; $i -le 3; $i++) {
        try {
            $rd = Post-Transfer $dupKey $headers $body
            if ($rd.saga_id -ne $r1.saga_id) { throw "duplicate returned different saga_id" }
        } catch {
            $code = $_.Exception.Response.StatusCode.value__
            if ($code -ne 409) { throw "duplicate expected 409 or same saga, got $code" }
            $sr = $_.ErrorDetails.Message | ConvertFrom-Json
            if ($sr.saga_id -and $sr.saga_id -ne $r1.saga_id) { throw "409 body has wrong saga_id" }
        }
    }
    Write-Host "   sagas created: $($sagaIds.Count) (+1 dup target)"

    Write-Host "== inject STEP_FAIL on all 6 =="
    foreach ($sid in ($sagaIds + $r1.saga_id)) {
        Invoke-RestMethod -Method Post "http://localhost:8081/transfers/$sid/fail" -Headers $headers | Out-Null
    }

    Write-Host "== assert all reach Compensated via gRPC-backed status =="
    $deadline = (Get-Date).AddSeconds(20)
    do {
        Start-Sleep -Milliseconds 300
        $states = foreach ($sid in ($sagaIds + $r1.saga_id)) {
            try { (Invoke-RestMethod "http://localhost:8081/sagas/$sid").state } catch { "pending" }
        }
        $done = ($states | Where-Object { $_ -eq "Compensated" }).Count
    } while ($done -lt 6 -and (Get-Date) -lt $deadline)
    if ($done -lt 6) { throw "only $done/6 Compensated: $($states -join ',')" }
    Write-Host "   6/6 Compensated"

    Write-Host "== kill -9 engine; restart; assert WAL replay =="
    Stop-Process -Id $eng.Id -Force  # kill -9 equivalent on Windows
    Start-Sleep -Seconds 1
    $eng2 = Start-Process -FilePath "$tmp\engine.exe" -PassThru -NoNewWindow
    $env:WAL_PATH = $null; $env:RING_PATH = $null; $env:ENGINE_ADDR = $null
    Start-Sleep -Seconds 3
    foreach ($sid in ($sagaIds + $r1.saga_id)) {
        $st = (Invoke-RestMethod "http://localhost:8081/sagas/$sid").state
        if ($st -ne "Compensated") { throw "after replay $sid state=$st want Compensated" }
    }
    Write-Host "   replay OK: 6/6 still Compensated after crash+restart"

    Write-Host "== auth negative checks =="
    try { Invoke-RestMethod -Method Post "http://localhost:8081/transfers" -Headers @{ Authorization = "Bearer bogus" } -ContentType "application/json" -Body "{}" ; throw "bogus JWT accepted" }
    catch { if ($_.Exception.Response.StatusCode.value__ -ne 401) { throw "want 401 got $($_.Exception.Response.StatusCode.value__)" } }
    Write-Host "   401 on bad JWT OK"

    Write-Host "TRACER PASS (Windows-native fallback)"
} finally {
    if ($eng -and -not $eng.HasExited) { Stop-Process -Id $eng.Id -Force -ErrorAction SilentlyContinue }
    if ($gw -and -not $gw.HasExited) { Stop-Process -Id $gw.Id -Force -ErrorAction SilentlyContinue }
    if ($eng2 -and -not $eng2.HasExited) { Stop-Process -Id $eng2.Id -Force -ErrorAction SilentlyContinue }
    Start-Sleep -Milliseconds 500
    Remove-Item $tmp -Recurse -Force -ErrorAction SilentlyContinue
}
