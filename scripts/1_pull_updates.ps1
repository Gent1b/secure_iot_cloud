# 1_pull_updates.ps1
# Pulls the latest code from GitHub on all VMs.
# Usage: .\scripts\1_pull_updates.ps1

$HOSTS = @("mqtt", "monitoring", "cloud", "devices")
$REPO_DIR = "secure_iot_cloud"
$BRANCH = "edge-branch"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " 1. GIT PULL (All Nodes)"
Write-Host "========================================" -ForegroundColor Cyan

foreach ($H in $HOSTS) {
    Write-Host ">>> Updating $H..." -ForegroundColor Yellow
    
    # Clean up Docker-owned files that Git can't delete (mosquitto configs, etc.)
    # We force remove any files that might have been created by containers
    ssh $H "cd $REPO_DIR && sudo chown -R `$(whoami):`$(whoami) . 2>/dev/null || true"
    
    # Reset to remote branch (handles force-pushes and divergent branches)
    ssh $H "cd $REPO_DIR && git fetch origin && git checkout $BRANCH && git reset --hard origin/$BRANCH"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H updated." -ForegroundColor Green
    } else {
        Write-Host "❌ $H update failed." -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " 2. DEPLOY ENVIRONMENT FILES"
Write-Host "========================================" -ForegroundColor Cyan

# Ensure /opt/iot-env directory exists on all VMs
foreach ($H in $HOSTS) {
    ssh $H "sudo mkdir -p /opt/iot-env && sudo chown ubuntu:ubuntu /opt/iot-env"
}

# Deploy environment files
Write-Host ">>> Deploying cloud.env..." -ForegroundColor Yellow
scp env-files/cloud.env cloud:/opt/iot-env/cloud.env

Write-Host ">>> Deploying monitor.env..." -ForegroundColor Yellow
scp env-files/monitor.env monitoring:/opt/iot-env/monitor.env

Write-Host ">>> Deploying mqtt.env..." -ForegroundColor Yellow
scp env-files/mqtt.env mqtt:/opt/iot-env/mqtt.env

Write-Host "✅ All nodes updated and environment files deployed." -ForegroundColor Green
