# Actual Runtime Architecture

This document clarifies **what actually runs where** in each scenario.

---

## VM Infrastructure (GCP)

| VM Name | Internal IP | Public IP | Role |
|---------|-------------|-----------|------|
| **devices** | 10.0.0.2 | 34.179.164.116 | Edge Layer (K3s cluster) |
| **cloud** | 10.0.0.3 | 35.198.89.149 | Cloud Layer (InfluxDB, Subscriber, Controller) |
| **mqtt** | 10.0.0.4 | 34.185.144.185 | Transport Layer (MQTT Broker) |
| **monitor** | 10.0.0.5 | 34.89.168.94 | Observability (Prometheus, Grafana) |

---

## Scenario A: Baseline (Cloud-Centric)

### Objective
Prove the problem: Centralized cloud architecture wastes bandwidth and resources.

### Data Flow
```
Sensors (50Hz) --MQTT--> Central Broker --MQTT--> Cloud Subscriber --> InfluxDB
```

### What Runs Where

#### **devices VM** (Docker)
- **publishers** (30 replicas)
  - File: [publisher/device.py](../publisher/device.py)
  - Config: `BROKER=34.185.144.185` (public IP), `PUBLISH_INTERVAL=0.02` (50Hz)
  - Publishes to: `iot/devices` topic on **central broker**

#### **mqtt VM** (Docker)
- **mosquitto** - MQTT broker accepting connections from devices
- **telegraf** - Metrics collector (Docker stats)

#### **cloud VM** (Docker)
- **influxdb** - Time-series database
- **subscriber** 
  - File: [subscriber/subscriber.py](../subscriber/subscriber.py)
  - Subscribes to: `iot/devices` topic
  - Writes to: InfluxDB `water_pipeline` measurement
- **node-exporter** - System metrics
- **telegraf** - Docker metrics

#### **monitor VM** (Docker)
- **prometheus** - Scrapes metrics from cloud/mqtt/devices
- **grafana** - Dashboards (http://34.89.168.94:3000)

---

## Scenario B: Static Edge (Bandwidth Reduction)

### Objective
Prove edge aggregation: 98% bandwidth reduction while preserving critical alerts.

### Data Flow
```
Sensors (50Hz) --MQTT--> Local Broker --> Edge Agent (aggregate) --MQTT--> Central Broker --MQTT--> Cloud Subscriber --> InfluxDB
```

### What Runs Where

#### **devices VM** (K3s Cluster)
- **Namespace:** `iot-edge`
- **Local MQTT Broker** (mosquitto pod)
  - ClusterIP service on port 1883
  - Local-only, not accessible from outside K3s
  
- **Publishers** (30 pods, 10 per site)
  - File: [publisher/device.py](../publisher/device.py)
  - Config: `BROKER=local-broker` (K3s service), `PUBLISH_INTERVAL=0.02` (50Hz)
  - Publishes to: `iot/devices` topic on **local broker**

- **Edge Agents** (3 pods, 1 per site: plant-a, plant-b, plant-c)
  - File: [subscriber/edge_agent.py](../subscriber/edge_agent.py)
  - **Dual MQTT clients:**
    - Input: Subscribes to `iot/devices` on `local-broker`
    - Output: Publishes to `iot/data/{site_id}` on `34.185.144.185` (central broker)
  - **Logic:**
    - Buffers 1 minute of data per device
    - Computes averages (pressure, flow, tank level)
    - **Immediate leak bypass:** Sends leak alerts instantly without buffering
  - **Mode:** Static `NORMAL` (1Hz aggregation)

#### **mqtt VM** (Docker)
- Same as baseline

#### **cloud VM** (Docker)
- **subscriber** changes:
  - Subscribes to: `iot/data/#` (wildcard, receives data from all 3 sites)
  - Receives pre-aggregated 1Hz data + instant leak alerts

#### **monitor VM** (Docker)
- Same as baseline

---

## Scenario C: Dynamic Edge (Feedback Loop)

### Objective
Prove adaptive system: Fairness algorithm dynamically allocates bandwidth based on leak events and cloud capacity.

### Data Flow
```
Sensors (50Hz) --MQTT--> Local Broker --> Edge Agent (adaptive) --MQTT--> Central Broker --MQTT--> Cloud Subscriber --> InfluxDB
                                                        ↑                                                       ↓
                                                        └─────────── Cloud Controller (feedback) ←─────────────┘
```

### What Runs Where

#### **devices VM** (K3s Cluster)
- **Edge Agents** (3 pods)
  - File: [subscriber/edge_agent.py](../subscriber/edge_agent.py)
  - **NEW: Feedback subscription:**
    - Subscribes to: `iot/control/{site_id}` on central broker
    - Receives commands: `{"mode": "NORMAL"}`, `{"mode": "DEBUG"}`, `{"mode": "ECONOMY"}`
  - **Dynamic behavior:**
    - `NORMAL` mode: 1-minute aggregation (1Hz output)
    - `DEBUG` mode: Raw passthrough (50Hz output)
    - `ECONOMY` mode: 5-minute aggregation (0.003Hz output)
  - **Leak override:** If leak detected locally, forced to DEBUG mode regardless of controller command

#### **mqtt VM** (Docker)
- Same as static

#### **cloud VM** (Docker)
- **controller** (NEW SERVICE)
  - File: [cloud-node/controller.py](../cloud-node/controller.py)
  - **Input sources:**
    1. InfluxDB: Queries `leak_flag` field (last 1 minute)
    2. Prometheus: Queries cloud CPU usage via node-exporter
  - **Decision logic (Max-Min Fairness):**
    - **Leak at site X?** → Site X forced to DEBUG, others to ECONOMY (priority override)
    - **Cloud CPU > 80%?** → All sites to ECONOMY (throttle to save resources)
    - **Cloud CPU < 20%?** → All sites to DEBUG (utilize spare capacity for training data)
  - **Output:** Publishes commands to `iot/control/{site_id}` every 10 seconds

- **subscriber** changes:
  - Same as static (receives processed data from edge agents)

#### **monitor VM** (Docker)
- Same as baseline

---

## Key Differences Summary

| Component | Baseline | Static Edge | Dynamic Edge |
|-----------|----------|-------------|--------------|
| **Publishers** | Direct to central MQTT | Via local MQTT | Via local MQTT |
| **Edge Agent** | ❌ None | ✅ Static (1Hz) | ✅ Adaptive (variable Hz) |
| **Feedback Loop** | ❌ None | ❌ None | ✅ Controller commands |
| **Bandwidth** | 50Hz × 30 sensors | 1Hz × 3 sites | Variable (0.003Hz to 50Hz) |
| **Cloud CPU** | High (50Hz ingestion) | Low (1Hz ingestion) | Dynamic (adapts to load) |

---

## File-to-Service Mapping

### Python Files
- **device.py** → Publisher pods/containers (sensors)
- **edge_agent.py** → Edge agent pods (K3s only)
- **subscriber.py** → Cloud subscriber container
- **controller.py** → Cloud controller container (dynamic scenario only)

### Docker Images
- **iot-publisher:local** - Built from `publisher/Dockerfile` (uses device.py)
- **iot-subscriber:local** - Built from `subscriber/Dockerfile` (contains both subscriber.py and edge_agent.py)
- **iot-controller:local** - Built from `cloud-node/Dockerfile.controller`

### Configuration Files
- **docker-compose.yml** - Baseline/static cloud services
- **docker-compose.dynamic.yml** - Dynamic cloud services (adds controller)
- **deployments/02_edge_static/k3s/edge-stack.yaml** - K3s edge deployment
- **deployments/03_edge_dynamic/k3s/** - Dynamic K3s edge deployment

---

## Common Confusion Points (Clarified)

### 1. "Why does subscriber.py still exist in edge scenarios?"
**Answer:** Edge agents use `edge_agent.py`, but both files live in the same Docker image (`iot-subscriber:local`). Cloud always uses `subscriber.py`, edge K3s pods override CMD to use `edge_agent.py`.

### 2. "Where does MODE='EDGE' come from?"
**Answer:** It doesn't. That code was removed. Subscriber now only runs in CLOUD mode. Edge processing happens in `edge_agent.py`.

### 3. "Why are there two MQTT brokers?"
**Answer:** Only in edge scenarios (static/dynamic). **Local broker** (K3s internal) handles 50Hz raw data. **Central broker** (mqtt VM) handles processed data + control commands. This creates a two-tier hierarchy.

### 4. "How does controller know about leaks?"
**Answer:** Controller queries InfluxDB for `leak_flag == 1` records (written by subscriber based on edge_agent-detected leaks). It doesn't directly subscribe to MQTT.

---

## Validation Commands

### Check what's running on devices VM:
```bash
# Baseline
ssh devices "docker ps"

# Static/Dynamic
ssh devices "kubectl get pods -n iot-edge"
```

### Check cloud services:
```bash
# Baseline/Static
ssh cloud "cd /root/secure_iot_cloud/cloud-node && docker compose ps"

# Dynamic
ssh cloud "cd /root/secure_iot_cloud/cloud-node && docker compose -f docker-compose.dynamic.yml ps"
```

### Check data flow:
```bash
# MQTT topics
mosquitto_sub -h 34.185.144.185 -t "iot/devices" -c 1     # Baseline raw data
mosquitto_sub -h 34.185.144.185 -t "iot/data/#" -c 1      # Edge processed data
mosquitto_sub -h 34.185.144.185 -t "iot/control/#" -c 1   # Controller commands

# InfluxDB
ssh cloud "docker exec cloud-node-influxdb-1 influx query 'from(bucket:\"iot_data\") |> range(start:-5m) |> filter(fn: (r) => r._measurement == \"water_pipeline\") |> limit(n:3)' -o secure_iot -t local_token_123"
```
