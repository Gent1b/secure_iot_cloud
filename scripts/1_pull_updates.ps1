# 1_pull_updates.ps1
# Pulls the latest code from GitHub on all VMs.
# Usage: .\scripts\1_pull_updates.ps1

$HOSTS = @("mqtt", "monitor", "cloud", "devices")
$REPO_DIR = "/root/secure_iot_cloud"
$BRANCH = "edge-branch"

Write-Host "========================================" -ForegroundColor Cyan
Write-Host " 1. GIT PULL (All Nodes)"
Write-Host "========================================" -ForegroundColor Cyan

foreach ($H in $HOSTS) {
    Write-Host ">>> Updating $H..." -ForegroundColor Yellow
    
    # Reset to remote branch (handles force-pushes and divergent branches)
    ssh $H "cd $REPO_DIR && git fetch origin && git checkout $BRANCH && git reset --hard origin/$BRANCH"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H updated." -ForegroundColor Green
    } else {
        Write-Host "❌ $H update failed." -ForegroundColor Red
    }
}

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host " 2. DEPLOY ENVIRONMENT FILE"
Write-Host "========================================" -ForegroundColor Cyan

# Deploy .env from local machine to ALL nodes via SCP
Write-Host ">>> Deploying .env to all nodes..." -ForegroundColor Yellow
foreach ($H in $HOSTS) {
    ssh $H "mkdir -p /opt/iot"
    scp .env ${H}:/opt/iot/.env
}

Write-Host "✅ All nodes updated and .env deployed." -ForegroundColor Green
