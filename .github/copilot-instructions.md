# Copilot Instructions: Secure IoT System with Edge Computing

## System Architecture

This is a **hierarchical 3-layer IoT system** for water infrastructure monitoring with dynamic resource allocation and feedback control. Deployed across 4 VMs:

| VM | Role | Components |
|----|------|------------|
| `devices` | **Edge Layer** | K3s cluster with 3 publishers (sensors) + 3 edge agents per site (plant-a, plant-b, plant-c) |
| `mqtt` | **Transport Layer** | Central MQTT broker (Mosquitto) - gateway between edge and cloud |
| `cloud` | **Cloud Layer** | InfluxDB (storage), Subscriber (ingestion), Controller (feedback orchestration) |
| `monitoring` | **Observability** | Prometheus + Grafana dashboards, Telegraf collectors |

### Data Flow
```
Sensors (50Hz raw) → Local Broker → Edge Agents (aggregate/filter) → Central Broker → Cloud Subscriber → InfluxDB
                                                        ↑                                         ↓
                                                        └─ Controller (feedback commands) ←─────┘
```

## Critical Concepts

### Dual-MQTT Architecture
- **Local Broker** (`localhost:1883` on edge VM): Sensors publish raw 50Hz data, edge agents consume it locally
- **Central Broker** (54.93.230.47:1883): Edge agents publish processed data upstream, subscribe to control commands

### Three Operating Modes (Edge Agents)
- **NORMAL** (1Hz): 1-minute aggregated averages - default, reduces bandwidth 98%
- **DEBUG** (50Hz): Raw passthrough - used during leak events or model training
- **ECONOMY** (5min): Heavy throttling - used when cloud is congested

### Fairness Algorithm (Cloud Controller)
The controller runs every 10 seconds and implements **Max-Min Fairness**:
1. Detects leaks via InfluxDB queries (leak_flag == 1)
2. Monitors cloud CPU from Prometheus node-exporter
3. Publishes mode commands to `iot/control/{site_id}` MQTT topic
4. Priority: Leak detection > Cloud capacity awareness

**State Transitions:**
- **Leak at Site X** → Site X forced to DEBUG mode, others may throttle
- **Cloud CPU < 20%** → All sites to DEBUG (maximize training data)
- **Cloud CPU > 80%** → Threatened sites throttle to ECONOMY

## Key Code Patterns

### Publisher (Sensor Simulation)
**File:** [publisher/device.py](../../publisher/device.py)
- Simulates physical water pump stations with realistic hydraulics
- Publishes to `iot/devices` topic every 20ms (50Hz)
- Uses site classification: `device_id → site_id` (deterministic based on device index)
- Does NOT directly communicate with cloud - local-only

**Key Functions:**
- `classify_device_site()`: Maps device IDs to plant sites (0-9 → plant-a, 10-19 → plant-b, etc.)
- `WaterPumpStation.step()`: Physics simulation (pressure, flow, tank level, leak detection)
- Leak injected via `simulate_leak()` when `LEAK_ACTIVE` flag set

### Edge Agent (Local Processing)
**File:** [subscriber/edge_agent.py](../../subscriber/edge_agent.py)
- **Input:** Subscribes to local broker's `iot/devices` topic
- **Processing:** Aggregates records based on `CURRENT_MODE`, detects leaks locally
- **Output:** Publishes processed data to `iot/data/{site_id}` on central broker
- **Commands:** Listens on `iot/control/{site_id}` for mode changes from controller

**Key Functions:**
- `detect_leak()`: Physics-based leak detection (flow > expected given pressure/valve)
- `aggregate_data()`: Compresses multiple records into averages based on mode
- Dual MQTT clients: local (input) + central (output/commands)

### Cloud Subscriber (Ingestion)
**File:** [subscriber/subscriber.py](../../subscriber/subscriber.py)
- Listens to `iot/data/#` on central broker
- Writes records to InfluxDB with fields: `pressure_psi`, `flow_gpm`, `tank_level`, `leak_flag`
- Includes Prometheus metrics: ingress/egress counters, gauge for operating mode

### Cloud Controller (Feedback)
**File:** [cloud-node/controller.py](../../cloud-node/controller.py)
- Queries InfluxDB for `leak_flag == 1` events (last 1 minute)
- Scrapes Prometheus for cloud CPU via node-exporter
- Publishes JSON commands: `{"mode": "DEBUG"}` to control topics
- Implements state machine: applies leak priority override > capacity threshold

## Infrastructure & Deployment

### Docker Compose Patterns
- **Baseline:** Single docker-compose.yml on each VM
- **Dynamic Scenario:** Uses `docker-compose.dynamic.yml` which includes controller service

**Critical Environment Variables:**
- `BROKER`: Central MQTT broker IP (e.g., 54.93.230.47)
- `INFLUXDB_TOKEN`, `INFLUXDB_ORG`, `INFLUXDB_BUCKET`: InfluxDB auth
- `SITE_ID`: Edge agent identifier (e.g., plant-a)
- `LOCAL_BROKER`, `CENTRAL_BROKER`: Broker IPs for dual-client setup

### K3s Deployment (Edge Scenario)
- Sensors/agents run as pods in K3s namespaces
- Local broker runs as a K3s service (ClusterIP)
- Deploy scripts in [deployments/](../../deployments/)

**Key Script:** [deployments/02_edge_static/deploy.sh](../../deployments/02_edge_static/deploy.sh)
- Installs K3s if not running
- Builds images locally (not pulled from registry for rapid iteration)
- Imports images into K3s containerd

### Monitoring Stack
- **Telegraf:** Collects Docker/system metrics, sends to Prometheus
- **Prometheus:** Scrapes metrics from Telegraf, Node-Exporter, edge agents (port 8000)
- **Grafana:** Visualizes system topology, throughput, latency via dashboards
- **Critical Fix:** Telegraf needs `group_add: [988]` to access docker.sock

## Common Workflows

### Deploy a Scenario (from local machine)
```powershell
.\scripts\4_deploy_scenario.ps1 -Scenario dynamic   # Options: baseline, static, dynamic
```

### Debug Edge Agent Issues
1. SSH to devices-node, check K3s logs: `kubectl logs -n default -l app=edge-agent`
2. Verify local broker connectivity: `mosquitto_sub -h edge-broker -t "iot/devices" -c 1`
3. Check central broker: `mosquitto_sub -h 54.93.230.47 -t "iot/data/plant-a" -c 1`

### Modify Edge Agent Logic
- Edit [subscriber/edge_agent.py](../../subscriber/edge_agent.py)
- Rebuild image: `docker build -t iot-subscriber:local subscriber/`
- Redeploy: `./deployments/02_edge_static/deploy.sh` (K3s) or docker-compose up

### Check System State
- **InfluxDB explorer:** Query `water_pipeline` measurement for leak_flag, pressure, flow
- **Grafana:** http://monitoring-vm:3000 - "IoT System Overview" dashboard
- **MQTT topics:** Manually subscribe to `iot/data/#` or `iot/control/#`

## Project-Specific Conventions

1. **Topic Naming:** `iot/{data|control}/{site_id}` - strict naming for cross-component routing
2. **Site IDs:** Always 7 chars: `plant-a`, `plant-b`, `plant-c` (used as MQTT topic segments)
3. **Time Windows:** 60s aggregation default, 10s controller loop - tuned for water system response times
4. **Metrics:** Prometheus port 8000 on each agent; use Counter/Gauge for aggregation compatibility
5. **JSON Commands:** Controller publishes `{"mode": "..."}` - keep payload flat for simplicity

## Common Pitfalls

- **Edge agents stuck in DEBUG mode:** Controller sends DEBUG during idle periods. Check cloud CPU query in `controller.py`.
- **Missing InfluxDB fields:** Subscriber errors on missing fields are non-critical but indicate data schema mismatch.
- **Telegraf docker.sock errors:** Add `group_add: [988]` to docker-compose.yml (already applied in dynamic/baseline).
- **K3s image import failures:** Ensure images are built locally with `-t iot-subscriber:local` and imported via `k3s ctr`.
- **MQTT connection timeouts:** Verify firewall rules allow 1883 between VMs; use `telnet <broker-ip> 1883`.

## Key Files to Understand First

1. [README.md](../../README.md) - Scenario descriptions and deployment quickstart
2. [docs/THESIS_ARCHITECTURE.md](../../docs/THESIS_ARCHITECTURE.md) - Design rationale and fairness algorithm
3. [docs/SYSTEM_OPERATION_GUIDE.md](../../docs/SYSTEM_OPERATION_GUIDE.md) - Current state, troubleshooting
4. [cloud-node/docker-compose.dynamic.yml](../../cloud-node/docker-compose.dynamic.yml) - Complete service stack
5. [subscriber/edge_agent.py](../../subscriber/edge_agent.py) - Core feedback loop implementation
