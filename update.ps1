# update.ps1
# Pulls latest code from git and restarts services.
# Usage: .\update.ps1

# Robust Start Order: Infrastructure -> Consumers -> Producers
$HOSTS = @("mqtt", "monitoring", "cloud", "devices")
$REPO_DIR = "secure_iot_cloud"
$BRANCH = "edge-branch"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " GLOBAL UPDATE (Git Pull + Restart)"
Write-Host " Order: MQTT -> Monitoring -> Cloud -> Devices"
Write-Host "========================================" -ForegroundColor Cyan

foreach ($H in $HOSTS) {
    Write-Host ""
    Write-Host ">>> Updating $H..." -ForegroundColor Yellow
    
    # 1. Update Code
    ssh $H "cd $REPO_DIR && git fetch origin && git checkout $BRANCH && git pull origin $BRANCH"
    if ($LASTEXITCODE -ne 0) {
        Write-Host "❌ Failed to update code on $H" -ForegroundColor Red
        continue
    }

    # 2. Restart/Build
    $CMD = ""
    switch ($H) {
        "mqtt"       { $CMD = "cd mqtt-node && docker compose up -d --remove-orphans" }
        "monitoring" { $CMD = "cd monitoring-node && docker compose up -d --remove-orphans" }
        "cloud"      { $CMD = "cd cloud-node && docker compose build && docker compose up -d --remove-orphans" }
        "devices"    { $CMD = "cd device-node && docker compose build && docker compose up -d --remove-orphans" }
    }

    ssh $H "cd $REPO_DIR && $CMD"
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H updated. Waiting 5s..." -ForegroundColor Green
        Start-Sleep -Seconds 5
    } else {
        Write-Host "❌ $H update failed." -ForegroundColor Red
    }
}
