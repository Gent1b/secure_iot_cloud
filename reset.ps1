# reset.ps1
# Hard Reset: Stops everything, wipes data volumes, and starts fresh.
# Usage: .\reset.ps1

$HOSTS = @("devices", "cloud", "monitoring", "mqtt")
$START_ORDER = @("mqtt", "monitoring", "cloud", "devices")
$REPO_DIR = "secure_iot_cloud"

Write-Host "========================================" -ForegroundColor Red
Write-Host " GLOBAL RESET (Down -v -> Up -d)"
Write-Host " ALL DATA WILL BE LOST!"
Write-Host "========================================" -ForegroundColor Red

# 1. TEARDOWN (Any order)
Write-Host "STEP 1: TEARING DOWN..." -ForegroundColor Yellow
foreach ($H in $HOSTS) {
    $CMD = ""
    switch ($H) {
        "mqtt"       { $CMD = "cd mqtt-node && docker compose down -v" }
        "monitoring" { $CMD = "cd monitoring-node && docker compose down -v" }
        "cloud"      { $CMD = "cd cloud-node && docker compose down -v" }
        "devices"    { $CMD = "cd device-node && docker compose down -v" }
    }
    ssh $H "cd $REPO_DIR && $CMD"
}

Write-Host "Waiting 5s..."
Start-Sleep -Seconds 5

# 2. STARTUP (Strict Order)
Write-Host "STEP 2: STARTING UP..." -ForegroundColor Yellow
foreach ($H in $START_ORDER) {
    Write-Host ">>> Starting $H..."
    $CMD = ""
    switch ($H) {
        "mqtt"       { $CMD = "cd mqtt-node && docker compose up -d" }
        "monitoring" { $CMD = "cd monitoring-node && docker compose up -d" }
        "cloud"      { $CMD = "cd cloud-node && docker compose up -d" }
        "devices"    { $CMD = "cd device-node && docker compose up -d" }
    }
    
    ssh $H "cd $REPO_DIR && $CMD"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H started. Waiting 5s..." -ForegroundColor Green
        Start-Sleep -Seconds 5
    } else {
        Write-Host "❌ $H failed to start." -ForegroundColor Red
    }
}

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " System Reset Complete"
Write-Host "========================================" -ForegroundColor Cyan
