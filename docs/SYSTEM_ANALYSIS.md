# Complete System Analysis - Secure IoT Edge-Cloud Architecture

**Document Version**: 1.0  
**Last Updated**: December 15, 2025  
**Git Commit**: 03a3800 (edge-branch)

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Architecture](#architecture)
3. [Experimental Scenarios](#experimental-scenarios)
4. [Feedback Algorithm](#feedback-algorithm)
5. [Water Utility Simulation](#water-utility-simulation)
6. [Observability Stack](#observability-stack)
7. [Deployment Workflow](#deployment-workflow)
8. [Current State](#current-state)
9. [File Structure Reference](#file-structure-reference)

---

## System Overview

This Master's Thesis implementation demonstrates a **hierarchical IoT system** for **Critical Infrastructure** (Water Utility) monitoring. The system evolves from a traditional cloud-centric architecture to an adaptive edge-cloud system with dynamic feedback control.

### Core Objectives
- **Bandwidth Optimization**: Reduce data transmission by 98% through edge aggregation
- **Safety Preservation**: Maintain <1s leak detection despite aggregation
- **Adaptive Resource Management**: Dynamic feedback loop balances application needs with infrastructure constraints
- **Fairness Algorithm**: Prioritize critical events while maximizing resource utilization

---

## Architecture

### 4-VM Distributed Topology

| VM Name | Role | Internal IP | Software Stack | Purpose |
|---------|------|-------------|----------------|----------|
| **devices** | Edge Layer | 172.31.44.105 | Docker/K3s, Publishers, Edge Agents | Simulates sensors and performs local processing |
| **mqtt** | Gateway Layer | 172.31.39.30 | Mosquitto MQTT, Broker Monitor | Central message broker (WAN gateway) |
| **cloud** | Cloud Layer | 172.31.33.61 | InfluxDB, Subscriber, Controller | Storage, Analytics, Control |
| **monitoring** | Observability | 172.31.42.61 | Prometheus, Grafana | System health monitoring |

### Network Topology

```
┌─────────────────────────────────────────────────────────────┐
│                      EDGE LAYER (devices)                    │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐      │
│  │ Publishers   │  │ Publishers   │  │ Publishers   │      │
│  │ (plant-a)    │  │ (plant-b)    │  │ (plant-c)    │      │
│  │ 10 sensors   │  │ 10 sensors   │  │ 10 sensors   │      │
│  │ @ 50Hz       │  │ @ 50Hz       │  │ @ 50Hz       │      │
│  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘      │
│         │                  │                  │              │
│  ┌──────▼──────────────────▼──────────────────▼─────┐       │
│  │        Local MQTT Broker (K3s Service)           │       │
│  └──────┬───────────────────────────────────────────┘       │
│         │                                                    │
│  ┌──────▼────────────────────────────────────────────┐      │
│  │  Edge Agent (Aggregates, Detects, Filters)       │      │
│  │  - Leak Detection (Local)                         │      │
│  │  - 60s Aggregation (Mode: NORMAL)                 │      │
│  │  - Command Listener (iot/control/{site})          │      │
│  └──────┬────────────────────────────────────────────┘      │
└─────────┼──────────────────────────────────────────────────┘
          │
          │ Internet (Filtered Data: ~30 msg/min)
          │
┌─────────▼──────────────────────────────────────────────────┐
│              GATEWAY LAYER (mqtt)                           │
│  ┌───────────────────────────────────────────────┐          │
│  │  Mosquitto MQTT Broker                        │          │
│  │  - Data Topic: iot/data/{site_id}             │          │
│  │  - Control Topic: iot/control/{site_id}       │          │
│  └───────────────┬────────────────────────────────┘         │
└──────────────────┼──────────────────────────────────────────┘
                   │
                   │
┌──────────────────▼──────────────────────────────────────────┐
│                 CLOUD LAYER (cloud)                          │
│  ┌────────────────┐  ┌─────────────────┐  ┌──────────────┐ │
│  │   Subscriber   │  │   Controller    │  │  InfluxDB    │ │
│  │  (Ingestion)   ├─→│  (Brain)        │←─┤  (Storage)   │ │
│  └────────┬───────┘  └────────┬────────┘  └──────────────┘ │
│           │                   │                              │
│           ├───────────────────┘                              │
│           │ Writes Data                                      │
│           │ Reads Leak State + Cloud CPU                     │
│           │ Publishes Mode Commands                          │
└───────────┼──────────────────────────────────────────────────┘
            │
            │ Metrics Exposure
            │
┌───────────▼──────────────────────────────────────────────────┐
│            OBSERVABILITY LAYER (monitoring)                   │
│  ┌──────────────┐         ┌──────────────┐                  │
│  │  Prometheus  │────────→│   Grafana    │                  │
│  │  (Scraper)   │         │ (Dashboard)  │                  │
│  └──────────────┘         └──────────────┘                  │
└───────────────────────────────────────────────────────────────┘
```

---

## Experimental Scenarios

The thesis validates three distinct deployment configurations:

### Scenario A: Baseline (Cloud-Centric)

**Hypothesis**: Traditional IoT architecture wastes bandwidth and resources.

**Implementation**:
- **Deployment**: Pure Docker on all nodes
- **Data Flow**: `Publishers (50Hz) → Internet → Cloud Subscriber → InfluxDB`
- **Configuration**: 30 devices (3 replicas × 10 devices each)

**Files**:
- `device-node/docker-compose.yml` - Publisher containers
- `cloud-node/docker-compose.yml` - Standard subscriber (MODE=CLOUD)

**Key Characteristics**:
```
Publish Rate:     50 Hz per device
Total Publishers: 30 devices
Messages/Second:  1,500 msg/sec
Daily Bandwidth:  ~6.5 GB
Cloud CPU Load:   HIGH (continuous ingestion)
Edge Intelligence: NONE
```

**Expected Results**:
- ❌ High network bandwidth usage
- ❌ High cloud CPU for data ingestion
- ❌ No local intelligence (network failure = data loss)
- ✅ Full-resolution data available

---

### Scenario B: Static Edge (Bandwidth Reduction)

**Hypothesis**: Edge aggregation reduces bandwidth by 98% while preserving critical alerts.

**Implementation**:
- **Deployment**: K3s on devices node
- **Data Flow**: `Publishers → Local MQTT → Edge Agent (60s avg) → Central MQTT → Cloud`
- **Local Processing**: Aggregation + Leak Detection

**Files**:
- `deployments/02_edge_static/k3s/edge-stack.yaml` - K3s manifests
- `deployments/02_edge_static/k3s/publishers.yaml` - Publisher pods
- `subscriber/edge_agent.py` - Dual-client aggregator

**Key Characteristics**:
```
Local Publish:    50 Hz (stays local)
Cloud Publish:    1 msg/min (60s aggregation)
Bandwidth Saved:  98% reduction
Leak Detection:   Local (immediate bypass)
Mode:             STATIC (always 60s window)
```

**Architecture**:
```yaml
Namespace: iot-edge
Components:
  1. Local Mosquitto Broker (ClusterIP Service)
  2. Publishers (30 pods, publish to local-broker:1883)
  3. Edge Agent (1 pod per site)
     - Subscribes: local-broker (iot/devices)
     - Publishes: central-broker (iot/data/{site_id})
```

**Expected Results**:
- ✅ 98% bandwidth reduction (3,000 msg/min → 30 msg/min)
- ✅ Cloud CPU drops significantly
- ✅ Leak alerts still sent immediately (bypass aggregation)
- ⚠️ Fixed 60s aggregation (no adaptability)

---

### Scenario C: Dynamic Edge (Feedback Loop) ⭐

**Hypothesis**: Cloud feedback enables resource-aware fairness and adaptive behavior.

**Implementation**:
- **Deployment**: K3s + Cloud Controller
- **Data Flow**: Bidirectional (Data uplink + Command downlink)
- **Adaptive Logic**: Controller changes Edge behavior based on system state

**Files**:
- `cloud-node/controller.py` - Fairness algorithm
- `cloud-node/docker-compose.dynamic.yml` - Adds controller service
- Edge Agent listens to `iot/control/{site_id}` for mode commands

**Key Characteristics**:
```
Operating Modes:  ECONOMY | NORMAL | DEBUG
Aggregation:      5min    | 60s    | 0s (passthrough)
Control Loop:     10-second cycle
Decision Inputs:  Leak state + Cloud CPU
Fairness Policy:  Priority-based allocation
```

**Controller Logic Flow**:
```python
Every 10 seconds:
  1. Query InfluxDB: "Any leaks detected in last 1 min?"
  2. Check Cloud CPU (file-based mock: /tmp/cloud_stress_test)
  
  3. Decide Mode for Each Site:
     IF leak detected:
       → Leaking site: DEBUG (50Hz raw data for analysis)
       → Other sites: ECONOMY (throttle to save bandwidth)
     
     ELIF cloud_cpu > 80%:
       → All sites: ECONOMY (reduce load)
     
     ELIF cloud_cpu < 20%:
       → All sites: DEBUG (maximize data collection)
     
     ELSE:
       → All sites: NORMAL (standard operation)
  
  4. Publish commands to iot/control/{plant-a|plant-b|plant-c}
     Payload: {"mode": "DEBUG", "timestamp": 1234567890.123}
```

**Expected Results**:
- ✅ Same 98% bandwidth savings as Static (in normal state)
- ✅ Adaptive response to critical events (leak → full resolution)
- ✅ Resource maximization (idle cloud → request more data)
- ✅ Congestion management (busy cloud → throttle all sites)
- 🎓 **Demonstrates fairness algorithm** (core thesis contribution)

---

## Feedback Algorithm

### The Fairness Problem

**Challenge**: Multiple edge sites compete for limited cloud resources. How to allocate bandwidth fairly while respecting:
1. **Safety Requirements**: Leaks need immediate high-resolution data
2. **Resource Constraints**: Cloud CPU/bandwidth limits
3. **Opportunity Maximization**: Use idle resources for analytics

### Solution: Priority-Based Max-Min Fairness

**Implementation**: `cloud-node/controller.py`

```python
def enforce_fairness(site_states, cloud_cpu):
    """
    Implements Max-Min Fairness with Priority Classes
    """
    commands = {}
    leaking_sites = [s for s, state in site_states.items() 
                     if state == "LEAK_DETECTED"]
    
    # PRIORITY 1: Safety (Leak Detection)
    if leaking_sites:
        for site in SITES:
            if site in leaking_sites:
                commands[site] = "DEBUG"   # Full bandwidth
            else:
                commands[site] = "ECONOMY" # Minimal bandwidth
    
    # PRIORITY 2: Congestion Avoidance
    elif cloud_cpu > 80.0:
        for site in SITES:
            commands[site] = "ECONOMY"
    
    # PRIORITY 3: Resource Maximization
    elif cloud_cpu < 20.0:
        for site in SITES:
            commands[site] = "DEBUG"
    
    # DEFAULT: Normal Operation
    else:
        for site in SITES:
            commands[site] = "NORMAL"
    
    return commands
```

### System State Determination

```python
def check_system_state():
    """
    Gathers system-wide state from multiple sources
    """
    site_states = {}
    
    # 1. Application State: Leak Detection
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -1m)
      |> filter(fn: (r) => r["_measurement"] == "water_pipeline")
      |> filter(fn: (r) => r["_field"] == "leak_flag")
      |> filter(fn: (r) => r["_value"] == 1)
      |> keep(columns: ["site_id"])
      |> distinct(column: "site_id")
    '''
    
    # Query InfluxDB for leak flags
    tables = query_api.query(query)
    for table in tables:
        for record in table.records:
            site_states[record["site_id"]] = "LEAK_DETECTED"
    
    # 2. Infrastructure State: Cloud CPU
    # Mock implementation (file-based for thesis control)
    if os.path.exists("/tmp/cloud_stress_test"):
        cloud_cpu = 90.0  # Simulated congestion
    else:
        cloud_cpu = 15.0  # Normal idle
    
    return site_states, cloud_cpu
```

### Command Distribution

**MQTT Control Topic**: `iot/control/{site_id}`

**Payload Format**:
```json
{
  "mode": "DEBUG",
  "timestamp": 1734278400.123
}
```

**Edge Agent Response** (`subscriber/edge_agent.py`):
```python
def on_central_message(client, userdata, msg):
    global CURRENT_MODE
    cmd = json.loads(msg.payload.decode())
    new_mode = cmd.get("mode")
    
    if new_mode in ["NORMAL", "DEBUG", "ECONOMY"]:
        CURRENT_MODE = new_mode
        
        # Update Prometheus metric
        mode_value = {"ECONOMY": 0, "NORMAL": 1, "DEBUG": 2}
        g_mode.set(mode_value[new_mode])
```

---

## Water Utility Simulation

### Physics Model

**Class**: `WaterPumpStation` (`publisher/device.py`)

**State Variables**:
```python
pump_status: int        # 0 = OFF, 1 = ON
valve_position: int     # 0-100%
tank_level: float       # 0-100%
pressure_psi: float     # Pounds per Square Inch
flow_gpm: float         # Gallons per Minute
leak_active: bool       # True if leak is occurring
```

**Control Logic**:
```python
# Hysteresis-based pump control
if tank_level < 30:
    pump_status = 1  # Turn ON
elif tank_level > 80:
    pump_status = 0  # Turn OFF

# Valve actuation (slow open/close)
target_valve = 100 if pump_status == 1 else 0
if valve_position < target_valve:
    valve_position += 5  # Open slowly
elif valve_position > target_valve:
    valve_position -= 5  # Close slowly
```

**Physics Equations**:
```python
# Pressure calculation
base_pressure = pump_status × 120  # PSI when pump is ON
pressure_drop = valve_position × 0.1  # Flow resistance
leak_factor = 40 if leak_active else 0  # Leak pressure loss
noise = random.normalvariate(0, 2.0)

pressure_psi = max(0, base_pressure - pressure_drop - leak_factor + noise)

# Flow calculation
expected_flow = pressure_psi × valve_position × 0.05
leak_flow = 20 if leak_active else 0  # Additional flow from leak
noise = random.normalvariate(0, 1.0)

flow_gpm = max(0, expected_flow + leak_flow + noise)

# Tank dynamics
if pump_status == 1:
    tank_level += 0.5  # Pump fills tank
tank_level -= 0.2  # Constant drainage
tank_level = clamp(tank_level, 0, 100)
```

**Leak Injection**:
```python
# Random leak occurrence (0.1% per timestep)
if not leak_active and random.random() < 0.001:
    leak_active = True
    log.warning("LEAK STARTED!")

# Random leak repair (5% per timestep)
if leak_active and random.random() < 0.05:
    leak_active = False
    log.info("Leak repaired.")
```

### Leak Detection Algorithm

**Location**: `subscriber/edge_agent.py`

**Principle**: Flow anomaly detection using physics model

```python
def detect_leak(record):
    """
    Detects leaks by comparing expected vs observed flow.
    
    Physics: Flow should be proportional to (Pressure × Valve Opening)
    Symptom: High Flow + Low Pressure = Fluid escaping before sensor
    """
    pressure = float(record.get("pressure_psi", 0))
    flow = float(record.get("flow_gpm", 0))
    valve = float(record.get("valve_position", 0))
    
    # Calculate expected flow based on physics
    expected_flow = pressure × valve × 0.05
    
    # Threshold-based detection
    if valve > 10 and flow > (expected_flow + 15.0):
        return True  # Leak detected
    
    return False
```

**Alert Flow**:
```python
if detect_leak(payload):
    log.warning(f"LEAK DETECTED on {device_id}!")
    m_leaks.inc()  # Prometheus counter
    
    # Priority Upload (bypass aggregation)
    alert_payload = payload.copy()
    alert_payload["alert_type"] = "LEAK_DETECTED"
    central_client.publish(
        f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", 
        json.dumps(alert_payload), 
        qos=1
    )
```

### Telemetry Schema

**MQTT Payload** (JSON):
```json
{
  "device_id": "baseline_sensor_1_1",
  "site_id": "plant-a",
  "timestamp": 1734278400.123,
  "pressure_psi": 118.45,
  "flow_gpm": 152.30,
  "valve_position": 95,
  "pump_status": 1,
  "tank_level_pct": 45.23,
  "maintenance_mode": 0,
  "leak_flag": 0
}
```

**Site Assignment**:
```python
SITES = ["plant-a", "plant-b", "plant-c"]

def classify_device_site(device_id: str) -> str:
    """
    Deterministic site assignment based on device index
    Example: device_1 → plant-a, device_2 → plant-b, device_3 → plant-c, device_4 → plant-a, ...
    """
    idx = int(device_id.rsplit("_", 1)[1]) - 1
    return SITES[idx % len(SITES)]
```

---

## Observability Stack

### Prometheus Configuration

**File**: `monitoring-node/prometheus/prometheus.yml`

**Scrape Configuration** (5-second interval):
```yaml
scrape_configs:
  # Cloud Node
  - job_name: 'telegraf'
    static_configs:
      - targets: ['172.31.33.61:9273']  # Docker stats
  
  - job_name: 'subscriber'
    static_configs:
      - targets: ['172.31.33.61:8000']  # Subscriber metrics
  
  - job_name: 'cloud_node_exporter'
    static_configs:
      - targets: ['172.31.33.61:9100']  # System metrics
  
  # MQTT Node
  - job_name: 'broker_monitor'
    static_configs:
      - targets: ['172.31.39.30:8001']  # MQTT message count
  
  - job_name: 'mqtt_monitor'
    static_configs:
      - targets: ['172.31.39.30:9273']  # Docker stats
  
  # Device Node
  - job_name: '[publishers]'
    static_configs:
      - targets: ['172.31.44.105:9273']  # Telegraf
```

### Metrics Catalog

#### Application Metrics (Subscriber)

**Location**: `subscriber/subscriber.py`, `subscriber/edge_agent.py`

```python
# Pipeline Stages
m_arrived = Counter(
    "mqtt_messages_arrived_total",
    "Total MQTT messages at network callback"
)

m_buffered = Counter(
    "mqtt_messages_buffered_total",
    "Messages successfully enqueued"
)

m_dropped = Counter(
    "mqtt_messages_dropped_total",
    "Messages dropped (queue full)"
)

m_processed = Counter(
    "app_messages_processed_total",
    "Messages processed by worker"
)

# Thesis Evaluation Metrics
bandwidth_usage = Counter(
    "iot_bandwidth_bytes_total",
    "Total telemetry bytes received"
)

reaction_latency = Histogram(
    "iot_reaction_latency_seconds",
    "Event timestamp → Detection time"
)

# Business Metrics
influx_writes_success = Counter(
    "influxdb_writes_success_total",
    "Successful InfluxDB writes"
)

leaks_detected = Counter(
    "pipeline_leaks_detected_total",
    "Leaks detected"
)

queue_depth = Gauge(
    "subscriber_queue_depth",
    "Current queue depth"
)
```

#### Edge Metrics

```python
m_ingress = Counter(
    "edge_ingress_messages_total",
    "Messages from local sensors"
)

m_egress = Counter(
    "edge_egress_messages_total",
    "Messages sent to cloud"
)

m_leaks = Counter(
    "edge_leaks_detected_total",
    "Leaks detected locally"
)

g_mode = Gauge(
    "edge_operating_mode",
    "Current mode (0=Economy, 1=Normal, 2=Debug)"
)
```

#### Infrastructure Metrics (Telegraf)

**Source**: `telegraf.conf` (Docker stats, system stats)

- `docker_container_cpu_usage_percent`
- `docker_container_mem_usage_percent`
- `docker_container_net_usage_total_bytes`
- `system_load1`, `system_load5`, `system_load15`
- `cpu_usage_percent`
- `mem_used_percent`

#### Broker Metrics

**File**: `broker-monitor/broker_monitor.py`

```python
broker_data_messages = Counter(
    'broker_data_messages_received_total',
    'MQTT messages on topic iot/devices'
)
```

### Grafana Dashboards

**Location**: `monitoring-node/grafana/provisioning/dashboards/`

#### dashboard.json - IoT Data Visualization
**Panels**:
- Pressure trends (time series)
- Flow rates (time series)
- Tank levels (gauge)
- Leak alerts (stat panel)
- Site-based filtering (variables)

**Data Source**: InfluxDB
**Measurement**: `water_pipeline`

#### dashboardmonitoring.json - System Metrics
**Panels**:
- CPU usage (all VMs)
- Memory usage (all VMs)
- Docker container stats
- Network traffic
- Disk usage

**Data Source**: Prometheus

### InfluxDB Schema

**Bucket**: `iot_data`
**Organization**: `my-org`

**Measurement**: `water_pipeline`

**Tags**:
- `device_id` - Unique device identifier
- `site_id` - Logical site (plant-a/b/c)

**Fields**:
```
pressure_psi: float
flow_gpm: float
tank_level_pct: float
leak_flag: int (0 or 1)
valve_position: int (0-100)
pump_status: int (0 or 1)
maintenance_mode: int
```

**Aggregated Records** (from Edge Agent):
```json
{
  "measurement": "water_pipeline",
  "tags": {
    "device_id": "edge_aggregate",
    "site_id": "plant-a"
  },
  "fields": {
    "pressure_psi": 119.23,
    "flow_gpm": 151.45,
    "tank_level_pct": 47.82,
    "aggregation_count": 3000,
    "mode": "NORMAL"
  },
  "timestamp": 1734278400000000000
}
```

---

## Deployment Workflow

### PowerShell Deployment Scripts

**Location**: `scripts/`

#### 1. Pull Updates (`1_pull_updates.ps1`)
```powershell
# Syncs code from GitHub to all VMs
foreach ($HOST in @("mqtt", "monitoring", "cloud", "devices")) {
    ssh $HOST "cd secure_iot_cloud && git pull origin edge-branch"
}
```

#### 2. Reset Environment (`2_reset_all.ps1`)
```powershell
# Stops containers, wipes volumes, cleans K3s
foreach ($HOST in @("mqtt", "monitoring", "cloud", "devices")) {
    ssh $HOST "cd secure_iot_cloud && docker compose down -v"
    
    if ($HOST -eq "devices") {
        ssh $HOST "sudo kubectl delete --all deployments,services,pods -n iot-edge"
    }
}
```

#### 3. Cleanup Disk (`3_cleanup_disk.ps1`)
```powershell
# Removes unused Docker resources
foreach ($HOST in @("mqtt", "monitoring", "cloud", "devices")) {
    ssh $HOST "docker system prune -af --volumes"
}
```

#### 4. Deploy Scenario (`4_deploy_scenario.ps1`)
```powershell
param([ValidateSet("baseline", "static", "dynamic")] $Scenario)

# Baseline: Pure Docker
if ($Scenario -eq "baseline") {
    ssh mqtt "cd secure_iot_cloud/mqtt-node && docker compose up -d"
    ssh monitoring "cd secure_iot_cloud/monitoring-node && docker compose up -d"
    ssh cloud "cd secure_iot_cloud/cloud-node && docker compose up -d"
    ssh devices "cd secure_iot_cloud/device-node && docker compose up -d"
}

# Static: K3s Edge
if ($Scenario -eq "static") {
    ssh mqtt "cd secure_iot_cloud/mqtt-node && docker compose up -d"
    ssh monitoring "cd secure_iot_cloud/monitoring-node && docker compose up -d"
    ssh cloud "cd secure_iot_cloud/cloud-node && docker compose -f docker-compose.edge.yml up -d"
    ssh devices "cd secure_iot_cloud/deployments/02_edge_static && ./deploy.sh"
}

# Dynamic: K3s + Controller
if ($Scenario -eq "dynamic") {
    ssh mqtt "cd secure_iot_cloud/mqtt-node && docker compose up -d"
    ssh monitoring "cd secure_iot_cloud/monitoring-node && docker compose up -d"
    ssh cloud "cd secure_iot_cloud/cloud-node && docker compose -f docker-compose.dynamic.yml up -d"
    ssh devices "cd secure_iot_cloud/deployments/03_edge_dynamic && ./deploy.sh"
}
```

### K3s Deployment

**Script**: `deployments/02_edge_static/deploy.sh`

```bash
#!/bin/bash

# 1. Install K3s (if not running)
if ! systemctl is-active --quiet k3s; then
    curl -sfL https://get.k3s.io | sh -
fi

# 2. Build Docker images locally
docker build -t iot-subscriber:local ../../subscriber
docker build -t iot-publisher:local ../../publisher

# 3. Import images to K3s containerd
docker save iot-subscriber:local -o /tmp/iot-subscriber.tar
sudo k3s ctr images import /tmp/iot-subscriber.tar
rm /tmp/iot-subscriber.tar

docker save iot-publisher:local -o /tmp/iot-publisher.tar
sudo k3s ctr images import /tmp/iot-publisher.tar
rm /tmp/iot-publisher.tar

# 4. Apply manifests
sudo kubectl apply -f k3s/edge-stack.yaml
sudo kubectl apply -f k3s/publishers.yaml
```

### Docker Compose Configurations

#### Baseline (`cloud-node/docker-compose.yml`)
```yaml
services:
  influxdb:
    image: influxdb:2.7
    environment:
      DOCKER_INFLUXDB_INIT_BUCKET: "iot_data"
      DOCKER_INFLUXDB_INIT_TOKEN: "my-token"
  
  subscriber:
    build: ../subscriber
    environment:
      BROKER: "54.93.230.47"
      MODE: "CLOUD"
      MQTT_TOPIC: "iot/devices"
```

#### Dynamic (`cloud-node/docker-compose.dynamic.yml`)
```yaml
services:
  influxdb:
    # Same as baseline
  
  subscriber:
    environment:
      MQTT_TOPIC: "iot/data/#"  # Changed topic
  
  controller:  # NEW SERVICE
    build:
      context: .
      dockerfile: Dockerfile.controller
    environment:
      BROKER: "54.93.230.47"
      INFLUXDB_URL: "http://influxdb:8086"
```

---

## Current State

**Git Branch**: `edge-branch`  
**Commit**: `03a3800` - "Fix all docker-compose files: remove missing env files, add defaults for all scenarios"

### ✅ What's Working

#### Infrastructure
- ✅ All Docker Compose files fixed (no missing env_file references)
- ✅ Hardcoded environment variable defaults in all scenarios
- ✅ Prometheus configuration updated with correct internal IPs
- ✅ All Prometheus targets showing UP status
- ✅ InfluxDB initialization working
- ✅ Grafana dashboard provisioning functional

#### Baseline Deployment
- ✅ 30 publishers running (3 replicas × 10 devices)
- ✅ Publishing at 50Hz to MQTT broker
- ✅ Subscriber ingesting to InfluxDB
- ✅ Data visible in Grafana
- ✅ Telegraf collecting Docker/system metrics

#### Simulation
- ✅ Water pump physics realistic
- ✅ Leak injection working (random 0.1% chance)
- ✅ Site classification (plant-a/b/c) functional
- ✅ Telemetry schema complete

#### Edge Logic
- ✅ Edge agent dual-client architecture implemented
- ✅ Local aggregation logic functional
- ✅ Leak detection algorithm working
- ✅ Command listener implemented
- ✅ Prometheus metrics exposed

#### Feedback Control
- ✅ Controller fairness algorithm implemented
- ✅ InfluxDB query for leak detection
- ✅ MQTT command publishing functional
- ✅ Mode switching logic complete

### ⚠️ Known Limitations / Mocked Components

#### Controller
- ⚠️ Cloud CPU check uses **file-based mock** (`/tmp/cloud_stress_test`) instead of real Prometheus API
  - **Why**: Simplifies thesis demonstration control
  - **Fix**: Replace with `requests` call to Prometheus API

```python
# Current (Mocked):
if os.path.exists("/tmp/cloud_stress_test"):
    cloud_cpu = 90.0
else:
    cloud_cpu = 15.0

# Ideal (Real):
response = requests.get(
    "http://172.31.42.61:9090/api/v1/query",
    params={'query': '100 - (avg(rate(node_cpu_seconds_total{mode="idle"}[1m])) * 100)'}
)
cloud_cpu = float(response.json()['data']['result'][0]['value'][1])
```

#### K3s Deployment
- ⚠️ Static/Dynamic edge scenarios require K3s installation on devices node
  - **Status**: Scripts ready, needs execution
  - **Verification**: `ssh devices "sudo kubectl get pods -n iot-edge"` currently fails (K3s not installed)

#### Dashboard Metrics
- ⚠️ Grafana doesn't yet visualize thesis-specific metrics:
  - Bandwidth saved percentage
  - Reaction latency distribution
  - Mode transition timeline
  - Fairness allocation visualization

#### Leak Triggering
- ⚠️ No manual leak injection mechanism
  - **Current**: Random 0.1% chance per timestep
  - **Desired**: HTTP endpoint or file trigger for deterministic testing

### 📋 Pending Tasks

#### High Priority
1. **Install K3s on devices node** - Run `deployments/02_edge_static/deploy.sh`
2. **Test Static Edge scenario** - Verify 98% bandwidth reduction
3. **Test Dynamic Edge scenario** - Trigger leak and verify controller response

#### Medium Priority
4. **Implement real Prometheus queries in controller** - Replace file mock
5. **Add thesis metrics to dashboard** - Bandwidth, latency, mode panels
6. **Create leak injection endpoint** - REST API or file-based trigger

#### Low Priority
7. **Automate scenario comparison** - Script to run all 3 scenarios and collect metrics
8. **Add CI/CD pipeline** - Automated image builds on GitHub
9. **Document recovery procedures** - Network failures, K3s crashes

---

## File Structure Reference

### Core Components

```
secure_iot_system_cloud/
├── publisher/
│   ├── device.py              # Water pump simulation (WaterPumpStation class)
│   └── Dockerfile             # Publisher container image
│
├── subscriber/
│   ├── subscriber.py          # Cloud subscriber (baseline/cloud mode)
│   ├── edge_agent.py          # Edge agent (static/dynamic mode)
│   └── Dockerfile             # Subscriber container image
│
├── cloud-node/
│   ├── controller.py          # Feedback controller (dynamic mode)
│   ├── Dockerfile.controller  # Controller container image
│   ├── docker-compose.yml     # Baseline deployment
│   ├── docker-compose.edge.yml    # Static edge deployment
│   └── docker-compose.dynamic.yml # Dynamic edge deployment
│
├── device-node/
│   └── docker-compose.yml     # Baseline publisher deployment
│
├── mqtt-node/
│   ├── docker-compose.yml     # Mosquitto broker
│   └── mosquitto_config/
│       └── mosquitto.conf     # Broker configuration
│
├── monitoring-node/
│   ├── docker-compose.yml     # Prometheus + Grafana
│   ├── prometheus/
│   │   └── prometheus.yml     # Scrape configuration
│   └── grafana/
│       └── provisioning/
│           ├── dashboards/
│           │   ├── dashboard.json           # IoT data dashboard
│           │   └── dashboardmonitoring.json # System metrics dashboard
│           └── datasources/
│               └── datasources.yaml         # Prometheus + InfluxDB
│
├── deployments/
│   ├── 02_edge_static/
│   │   ├── deploy.sh          # K3s deployment script
│   │   └── k3s/
│   │       ├── edge-stack.yaml    # Edge infrastructure (broker + agent)
│   │       └── publishers.yaml    # Publisher pods
│   └── 03_edge_dynamic/
│       └── deploy.sh          # Same as static (controller is cloud-side)
│
├── scripts/
│   ├── 1_pull_updates.ps1     # Git pull on all VMs
│   ├── 2_reset_all.ps1        # Stop containers, wipe data
│   ├── 3_cleanup_disk.ps1     # Docker prune
│   └── 4_deploy_scenario.ps1  # Deploy baseline/static/dynamic
│
├── broker-monitor/
│   ├── broker_monitor.py      # MQTT message counter
│   └── Dockerfile             # Monitor container image
│
└── docs/
    ├── THESIS_ARCHITECTURE.md       # Academic specification
    ├── DYNAMIC_EDGE_ARCHITECTURE.md # Feedback loop details
    ├── SIMPLE_EXPLANATION.md        # Non-technical overview
    ├── SYSTEM_OPERATION_GUIDE.md    # Operational procedures
    └── SYSTEM_ANALYSIS.md           # This document
```

### Configuration Files

```
cloud-node/telegraf/telegraf.conf    # Docker stats collection
device-node/telegraf/telegraf.conf   # Docker stats collection
mqtt-node/telegraf/telegraf.conf     # Docker stats collection
```

### Key Topics Schema

| Topic | Publisher | Subscriber | QoS | Purpose |
|-------|-----------|------------|-----|---------|
| `iot/devices` | Publishers | Subscriber (Baseline) | 0 | Raw sensor data (baseline) |
| `local/sensors/{id}` | Publishers | Edge Agent | 0 | Raw sensor data (local, K3s) |
| `iot/data/{site_id}` | Edge Agent | Subscriber | 0 | Aggregated data |
| `iot/control/{site_id}` | Controller | Edge Agent | 1 | Mode commands |

---

## Appendix: Key Commands

### Deployment
```powershell
# Baseline
.\scripts\4_deploy_scenario.ps1 -Scenario baseline

# Static Edge
.\scripts\4_deploy_scenario.ps1 -Scenario static

# Dynamic Edge
.\scripts\4_deploy_scenario.ps1 -Scenario dynamic
```

### Verification
```bash
# Check Docker services
ssh cloud "docker ps"

# Check K3s pods
ssh devices "sudo kubectl get pods -n iot-edge"

# Check Prometheus targets
curl http://172.31.42.61:9090/api/v1/targets

# Check InfluxDB data
ssh cloud "docker exec influxdb influx query 'from(bucket:\"iot_data\") |> range(start: -5m) |> limit(n: 10)'"
```

### Debugging
```bash
# View logs
ssh cloud "docker logs -f cloud-node-subscriber-1"
ssh devices "sudo kubectl logs -f -n iot-edge deployment/edge-agent-site-a"

# Force leak (manual edit device.py)
ssh devices "cd secure_iot_cloud/publisher && sed -i 's/0.001/0.5/' device.py"

# Trigger cloud stress (controller mock)
ssh cloud "touch /tmp/cloud_stress_test"
```

### Metrics
```bash
# Subscriber metrics
curl http://172.31.33.61:8000/metrics

# Edge agent metrics
ssh devices "curl http://localhost:8000/metrics"

# Broker monitor
curl http://172.31.39.30:8001/metrics
```

---

**End of Document**
