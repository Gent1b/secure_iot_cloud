<#
4_deploy_scenario.ps1
Deploys one of the 3 thesis scenarios.

Usage:
    .\scripts\4_deploy_scenario.ps1 -Scenario baseline
    .\scripts\4_deploy_scenario.ps1 -Scenario static
    .\scripts\4_deploy_scenario.ps1 -Scenario dynamic
#>

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("baseline", "static", "dynamic")]
    [string]$Scenario,

    [string]$RunId
)

$REPO_DIR = "/root/secure_iot_cloud"

Write-Host "========================================" -ForegroundColor Green
Write-Host " 4. DEPLOY SCENARIO: $Scenario"
Write-Host "========================================" -ForegroundColor Green

# --- SCENARIO A: BASELINE ---
if ($Scenario -eq "baseline") {
    Write-Host ">>> Starting Baseline (Docker on all nodes)..."

    $RUN_ID = if ($RunId) { $RunId } else { "baseline-" + (Get-Date -Format "yyyyMMdd-HHmmss") }
    $SCENARIO_TAG = "baseline"
    Write-Host ">>> RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG" -ForegroundColor Cyan
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitor "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Standard) - source env first
    ssh cloud "set -a; source /opt/iot/.env; set +a; cd $REPO_DIR/cloud-node; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG docker compose -f docker-compose.yml up -d"
    
    # 4. Devices (Docker) - Set environment variables for baseline
    ssh devices "cd $REPO_DIR/device-node; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG docker compose up -d"
}

# --- SCENARIO B: STATIC EDGE ---
if ($Scenario -eq "static") {
    Write-Host ">>> Starting Static Edge (K3s on Devices)..."

    $RUN_ID = if ($RunId) { $RunId } else { "static-" + (Get-Date -Format "yyyyMMdd-HHmmss") }
    $SCENARIO_TAG = "static"
    Write-Host ">>> RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG" -ForegroundColor Cyan
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitor "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Edge Mode) - source env first
    ssh cloud "set -a; source /opt/iot/.env; set +a; cd $REPO_DIR/cloud-node; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG docker compose -f docker-compose.edge.yml up -d"
    
    # 4. Devices (K3s)
    # Ensure script is executable and run it (fix line endings first)
    ssh devices "cd $REPO_DIR/deployments/02_edge_static; sed -i 's/\r$//' deploy.sh; chmod +x deploy.sh; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG ./deploy.sh"
}

# --- SCENARIO C: DYNAMIC EDGE ---
if ($Scenario -eq "dynamic") {
    Write-Host ">>> Starting Dynamic Edge (Feedback Loop)..."

    $RUN_ID = if ($RunId) { $RunId } else { "dynamic-" + (Get-Date -Format "yyyyMMdd-HHmmss") }
    $SCENARIO_TAG = "adaptive"
    Write-Host ">>> RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG" -ForegroundColor Cyan
    
    # 1. MQTT
    Write-Host ">>> Starting MQTT broker..." -ForegroundColor Yellow
    ssh mqtt "cd $REPO_DIR/mqtt-node; docker compose up -d"
    
    # 2. Monitoring
    Write-Host ">>> Starting Monitoring stack..." -ForegroundColor Yellow
    ssh monitor "cd $REPO_DIR/monitoring-node; docker compose up -d"
    
    # 3. Cloud (Dynamic Mode + Controller) - source env first
    Write-Host ">>> Starting Cloud services (subscriber + controller)..." -ForegroundColor Yellow
    ssh cloud "set -a; source /opt/iot/.env; set +a; cd $REPO_DIR/cloud-node; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG docker compose -f docker-compose.dynamic.yml up -d"
    
    # 4. Devices (K3s Dynamic)
    Write-Host ">>> Deploying edge agents to K3s..." -ForegroundColor Yellow
    ssh devices "cd $REPO_DIR/deployments/03_edge_dynamic; sed -i 's/\r$//' deploy.sh; chmod +x deploy.sh; RUN_ID=$RUN_ID SCENARIO=$SCENARIO_TAG ./deploy.sh"
}

Write-Host "✅ Deployment command sent. Check Grafana/InfluxDB for data." -ForegroundColor Green
