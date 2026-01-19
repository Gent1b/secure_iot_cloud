<#
3_cleanup_disk.ps1
Prunes unused Docker images and system data to free up space.

Usage:
    .\scripts\3_cleanup_disk.ps1
    .\scripts\3_cleanup_disk.ps1 -Hosts cloud,mqtt
#>

[CmdletBinding()]
param(
        [string[]]$Hosts = @("mqtt", "monitor", "cloud", "devices")
)

Write-Host "========================================" -ForegroundColor Magenta
Write-Host " 3. DISK CLEANUP (Docker Prune)"
Write-Host "========================================" -ForegroundColor Magenta

foreach ($H in $Hosts) {
    Write-Host ">>> Cleaning $H..." -ForegroundColor Yellow

    # Some nodes (e.g., k3s/containerd) may not have Docker.
    ssh $H "if command -v docker >/dev/null 2>&1; then docker system prune -a -f --volumes; else echo 'docker not found; skipping docker prune'; fi"
    
    # Show free space
    $SPACE = ssh $H "df -h / | tail -n 1 | awk '{print \$4}'"
    Write-Host "✅ $H cleaned. Free Space: $SPACE" -ForegroundColor Green
}
