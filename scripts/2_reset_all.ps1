# 2_reset_all.ps1
# Stops all containers, removes volumes, and cleans up K3s resources.
# Usage: .\scripts\2_reset_all.ps1

$HOSTS = @("mqtt", "monitoring", "cloud", "devices")
$REPO_DIR = "secure_iot_cloud"

Write-Host "========================================" -ForegroundColor Red
Write-Host " 2. GLOBAL RESET (Stop & Wipe)"
Write-Host "========================================" -ForegroundColor Red

foreach ($H in $HOSTS) {
    Write-Host ">>> Resetting $H..." -ForegroundColor Yellow
    
    # 1. Docker Compose Down (Wipe Volumes)
    # We try to down ALL potential compose files to be safe
    $CMD_DOCKER = "cd $REPO_DIR && \
        (cd mqtt-node && docker compose down -v 2>/dev/null || true) && \
        (cd monitoring-node && docker compose down -v 2>/dev/null || true) && \
        (cd cloud-node && docker compose -f docker-compose.yml down -v 2>/dev/null || true) && \
        (cd cloud-node && docker compose -f docker-compose.edge.yml down -v 2>/dev/null || true) && \
        (cd cloud-node && docker compose -f docker-compose.dynamic.yml down -v 2>/dev/null || true) && \
        (cd device-node && docker compose down -v 2>/dev/null || true)"

    ssh $H $CMD_DOCKER

    # 2. K3s Cleanup (Only on devices node)
    if ($H -eq "devices") {
        Write-Host "    Cleaning K3s resources on devices..."
        ssh $H "sudo kubectl delete --all deployments,services,pods,configmaps -n iot-edge 2>/dev/null || true"
    }
    
    Write-Host "✅ $H reset complete." -ForegroundColor Green
}
