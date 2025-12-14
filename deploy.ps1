param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("baseline", "static-edge", "dynamic-edge")]
    [string]$Scenario
)

Write-Host "Deploying Scenario: $Scenario" -ForegroundColor Green

switch ($Scenario) {
    "baseline" {
        Write-Host "Starting Baseline Scenario (Docker Compose on all nodes)..."
        # ssh mqtt "cd /opt/iot-system/mqtt-node && docker-compose up -d"
        # ssh devices "cd /opt/iot-system/device-node && docker-compose up -d"
        # ssh cloud "cd /opt/iot-system/cloud-node && docker-compose up -d"
        Write-Host "Commands to run manually (until SSH keys are fully automated):"
        Write-Host "1. [mqtt] cd mqtt-node && docker-compose up -d"
        Write-Host "2. [cloud] cd cloud-node && docker-compose up -d"
        Write-Host "3. [devices] cd device-node && docker-compose up -d"
    }
    "static-edge" {
        Write-Host "Starting Static Edge Scenario (K3s on devices, Docker on others)..."
        # ssh mqtt "cd /opt/iot-system/mqtt-node && docker-compose up -d"
        # ssh devices "cd /opt/iot-system && ./deployments/02_edge_static/deploy.sh"
        # ssh cloud "cd /opt/iot-system && MODE=EDGE_CLOUD docker-compose -f cloud-node/docker-compose.edge.yml up -d"
        Write-Host "Commands to run manually:"
        Write-Host "1. [mqtt] cd mqtt-node && docker-compose up -d"
        Write-Host "2. [cloud] cd cloud-node && docker-compose -f docker-compose.edge.yml up -d"
        Write-Host "3. [devices] cd deployments/02_edge_static && ./deploy.sh"
    }
    "dynamic-edge" {
        Write-Host "Starting Dynamic Edge Scenario (Feedback Loop)..."
        # ssh mqtt "cd /opt/iot-system/mqtt-node && docker-compose up -d"
        # ssh devices "cd /opt/iot-system && ./deployments/03_edge_dynamic/deploy.sh"
        # ssh cloud "cd /opt/iot-system && docker-compose -f cloud-node/docker-compose.dynamic.yml up -d"
        Write-Host "Commands to run manually:"
        Write-Host "1. [mqtt] cd mqtt-node && docker-compose up -d"
        Write-Host "2. [cloud] cd cloud-node && docker-compose -f docker-compose.dynamic.yml up -d"
        Write-Host "3. [devices] cd deployments/03_edge_dynamic && ./deploy.sh"
    }
}
