# cleanup.ps1
# Nukes unused Docker images, containers, and networks on all nodes to free up space.
# Usage: .\cleanup.ps1

$HOSTS = @("devices", "cloud", "monitoring", "mqtt")

Write-Host "========================================" -ForegroundColor Red
Write-Host " GLOBAL DOCKER CLEANUP"
Write-Host " WARNING: This will delete all stopped containers and unused images!"
Write-Host "========================================" -ForegroundColor Red

foreach ($H in $HOSTS) {
    Write-Host ""
    Write-Host ">>> Cleaning $H..." -ForegroundColor Yellow
    
    # 1. Prune everything (images, containers, networks)
    # 2. Also remove anonymous volumes
    ssh $H "docker system prune -a -f --volumes"
    
    if ($LASTEXITCODE -eq 0) {
        Write-Host "✅ $H cleaned successfully." -ForegroundColor Green
    } else {
        Write-Host "❌ $H cleanup failed." -ForegroundColor Red
    }
    
    # Check disk usage after
    ssh $H "df -h / | tail -n 1"
}
