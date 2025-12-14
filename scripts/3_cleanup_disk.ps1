# 3_cleanup_disk.ps1
# Prunes unused Docker images and system data to free up space.
# Usage: .\scripts\3_cleanup_disk.ps1

$HOSTS = @("mqtt", "monitoring", "cloud", "devices")

Write-Host "========================================" -ForegroundColor Magenta
Write-Host " 3. DISK CLEANUP (Docker Prune)"
Write-Host "========================================" -ForegroundColor Magenta

foreach ($H in $HOSTS) {
    Write-Host ">>> Cleaning $H..." -ForegroundColor Yellow
    ssh $H "docker system prune -a -f --volumes"
    
    # Show free space
    $SPACE = ssh $H "df -h / | tail -n 1 | awk '{print \$4}'"
    Write-Host "✅ $H cleaned. Free Space: $SPACE" -ForegroundColor Green
}
