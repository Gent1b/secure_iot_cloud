# 4_deploy_scenario.ps1
# Deploys one of the 3 thesis scenarios.
# Usage: .\scripts\4_deploy_scenario.ps1 -Scenario [baseline|static|dynamic]

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("baseline", "static", "dynamic")]
    [string]$Scenario
)

$REPO_DIR = "/root/secure_iot_cloud"

Write-Host "========================================" -ForegroundColor Green
Write-Host " 4. DEPLOY SCENARIO: $Scenario"
Write-Host "========================================" -ForegroundColor Green

# --- SCENARIO A: BASELINE ---
if ($Scenario -eq "baseline") {
    Write-Host ">>> Starting Baseline (Docker on all nodes)..."
    
    # 0. Deploy environment file
    Write-Host ">>> Deploying .env file..." -ForegroundColor Yellow
    foreach ($node in @("cloud", "monitor", "mqtt")) {
        ssh $node "mkdir -p /opt/iot && cp /root/secure_iot_cloud/.env /opt/iot/.env"
    }
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitor "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Standard)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.yml up -d"
    
    # 4. Devices (Docker) - Set environment variables for baseline
    ssh devices "cd $REPO_DIR/device-node && BROKER=34.185.144.185 MQTT_TOPIC=iot/devices DEVICES_PER_CONTAINER=10 PUBLISH_INTERVAL=0.02 docker compose up -d"
}

# --- SCENARIO B: STATIC EDGE ---
if ($Scenario -eq "static") {
    Write-Host ">>> Starting Static Edge (K3s on Devices)..."
    
    # 0. Deploy environment file
    Write-Host ">>> Deploying .env file..." -ForegroundColor Yellow
    foreach ($node in @("cloud", "monitor", "mqtt", "devices")) {
        ssh $node "mkdir -p /opt/iot && cp /root/secure_iot_cloud/.env /opt/iot/.env"
    }
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitor "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Edge Mode)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.edge.yml up -d"
    
    # 4. Devices (K3s)
    # Ensure script is executable and run it (fix line endings first)
    ssh devices "cd $REPO_DIR/deployments/02_edge_static && sed -i 's/\r$//' deploy.sh && chmod +x deploy.sh && ./deploy.sh"
}

# --- SCENARIO C: DYNAMIC EDGE ---
if ($Scenario -eq "dynamic") {
    Write-Host ">>> Starting Dynamic Edge (Feedback Loop)..."
    
    # 1. MQTT
    Write-Host ">>> Starting MQTT broker..." -ForegroundColor Yellow
    ssh mqtt "cd $REPO_DIR/mqtt-node; docker compose up -d"
    
    # 2. Monitoring
    Write-Host ">>> Starting Monitoring stack..." -ForegroundColor Yellow
    ssh monitor "cd $REPO_DIR/monitoring-node; docker compose up -d"
    
    # 3. Cloud (Dynamic Mode + Controller)
    Write-Host ">>> Starting Cloud services (subscriber + controller)..." -ForegroundColor Yellow
    ssh cloud "cd $REPO_DIR/cloud-node; docker compose -f docker-compose.dynamic.yml up -d"
    
    # 4. Devices (K3s Dynamic)
    Write-Host ">>> Deploying edge agents to K3s..." -ForegroundColor Yellow
    ssh devices "cd $REPO_DIR/deployments/03_edge_dynamic; sed -i 's/\r$//' deploy.sh; sed -i 's/\r$//' ../02_edge_static/deploy.sh; chmod +x ../02_edge_static/deploy.sh; chmod +x deploy.sh; ./deploy.sh"
}

Write-Host "✅ Deployment command sent. Check Grafana/InfluxDB for data." -ForegroundColor Green
