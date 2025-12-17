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
    
    # 0. Deploy environment files
    Write-Host ">>> Deploying environment files..."
    ssh cloud "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/cloud.env /opt/iot-env/cloud.env"
    ssh monitoring "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/monitor.env /opt/iot-env/monitor.env"
    ssh mqtt "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/mqtt.env /opt/iot-env/mqtt.env"
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Standard)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.yml up -d"
    
    # 4. Devices (Docker) - Set environment variables for baseline
    ssh devices "cd $REPO_DIR/device-node && BROKER=54.93.230.47 MQTT_TOPIC=iot/devices DEVICES_PER_CONTAINER=10 PUBLISH_INTERVAL=0.02 docker compose up -d"
}

# --- SCENARIO B: STATIC EDGE ---
if ($Scenario -eq "static") {
    Write-Host ">>> Starting Static Edge (K3s on Devices)..."
    
    # 0. Deploy environment files
    Write-Host ">>> Deploying environment files..."
    ssh cloud "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/cloud.env /opt/iot-env/cloud.env"
    ssh monitoring "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/monitor.env /opt/iot-env/monitor.env"
    ssh mqtt "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/mqtt.env /opt/iot-env/mqtt.env"
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Edge Mode)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.edge.yml up -d"
    
    # 4. Devices (K3s)
    # Ensure script is executable and run it (fix line endings first)
    ssh devices "cd $REPO_DIR/deployments/02_edge_static && sed -i 's/\r$//' deploy.sh && chmod +x deploy.sh && ./deploy.sh"
}

# --- SCENARIO C: DYNAMIC EDGE ---
if ($Scenario -eq "dynamic") {
    Write-Host ">>> Starting Dynamic Edge (Feedback Loop)..."
    
    # 0. Deploy environment files
    Write-Host ">>> Deploying environment files..."
    ssh cloud "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/cloud.env /opt/iot-env/cloud.env"
    ssh monitoring "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/monitor.env /opt/iot-env/monitor.env"
    ssh mqtt "sudo mkdir -p /opt/iot-env && sudo cp ~/secure_iot_cloud/env-files/mqtt.env /opt/iot-env/mqtt.env"
    
    # 1. MQTT
    ssh mqtt "cd $REPO_DIR/mqtt-node && docker compose up -d"
    
    # 2. Monitoring
    ssh monitoring "cd $REPO_DIR/monitoring-node && docker compose up -d"
    
    # 3. Cloud (Dynamic Mode + Controller)
    ssh cloud "cd $REPO_DIR/cloud-node && docker compose -f docker-compose.dynamic.yml up -d --build"
    
    # 4. Devices (K3s Dynamic)
    # We ensure both the current script and the referenced static script are executable and have correct line endings
    ssh devices "cd $REPO_DIR/deployments/03_edge_dynamic && sed -i 's/\r$//' deploy.sh && sed -i 's/\r$//' ../02_edge_static/deploy.sh && chmod +x ../02_edge_static/deploy.sh && chmod +x deploy.sh && ./deploy.sh"
}

Write-Host "✅ Deployment command sent. Check Grafana/InfluxDB for data." -ForegroundColor Green
