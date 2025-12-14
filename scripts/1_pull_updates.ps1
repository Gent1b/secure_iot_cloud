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
    
    # We use git reset --hard to discard any local changes (like sed fixes) that might conflict with the pull
    ssh $H "cd $REPO_DIR && git fetch origin && git reset --hard HEAD && git checkout $BRANCH && git pull origin $BRANCH"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H updated." -ForegroundColor Green
    } else {
        Write-Host "❌ $H update failed." -ForegroundColor Red
    }
}
