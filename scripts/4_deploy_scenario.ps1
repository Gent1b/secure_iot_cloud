# 4_deploy_scenario.ps1
# Deploys one of the 3 thesis scenarios.
# Usage: .\scripts\4_deploy_scenario.ps1 -Scenario [baseline|static|dynamic]

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("baseline", "static", "dynamic")]
    [string]$Scenario
)

$REPO_DIR = "secure_iot_cloud"

Write-Host "========================================" -ForegroundColor Green
Write-Host " 4. DEPLOY SCENARIO: $Scenario"
Write-Host "========================================" -ForegroundColor Green

# --- SCENARIO A: BASELINE ---
if ($Scenario -eq "baseline") {
    Write-Host ">>> Starting Baseline (Docker on all nodes)..."
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Standard)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.yml up -d"
    
    # 4. Devices (Docker)
    ssh devices "cd $REPO_DIR/device-node && docker compose up -d"
}

# --- SCENARIO B: STATIC EDGE ---
if ($Scenario -eq "static") {
    Write-Host ">>> Starting Static Edge (K3s on Devices)..."
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Edge Mode)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.edge.yml up -d"
    
    # 4. Devices (K3s)
    # Ensure script is executable and run it
    ssh devices "cd $REPO_DIR/deployments/02_edge_static && chmod +x deploy.sh && ./deploy.sh"
}

# --- SCENARIO C: DYNAMIC EDGE ---
if ($Scenario -eq "dynamic") {
    Write-Host ">>> Starting Dynamic Edge (Feedback Loop)..."
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Dynamic Mode + Controller)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.dynamic.yml up -d --build"
    
    # 4. Devices (K3s Dynamic)
    ssh devices "cd $REPO_DIR/deployments/03_edge_dynamic && chmod +x deploy.sh && ./deploy.sh"
}

Write-Host "✅ Deployment command sent. Check Grafana/InfluxDB for data." -ForegroundColor Green
