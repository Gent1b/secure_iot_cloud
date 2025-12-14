# System Operation Guide: Dynamic Edge-Cloud IoT System

## Table of Contents
1. [System Overview](#system-overview)
2. [Current System State](#current-system-state)
3. [Component Breakdown](#component-breakdown)
4. [Data Flow Walkthrough](#data-flow-walkthrough)
5. [Operating Modes](#operating-modes)
6. [Controller Decision Logic](#controller-decision-logic)
7. [Performance Metrics](#performance-metrics)
8. [Troubleshooting](#troubleshooting)

---

## System Overview

You have successfully deployed a **3-Layer Hierarchical IoT Architecture** for water infrastructure monitoring with dynamic resource allocation.

### Architecture Layers

```
┌─────────────────────────────────────────────────────────────────┐
│ Layer 3: Cloud (cloud-node VM)                                  │
│ ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│ │  Controller  │  │  Subscriber  │  │  InfluxDB    │           │
│ │  (Feedback)  │  │  (Ingestion) │  │  (Storage)   │           │
│ └──────────────┘  └──────────────┘  └──────────────┘           │
└─────────────────────────────────────────────────────────────────┘
                            ▲
                            │ MQTT (Central Broker)
                            │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 2: Transport (mqtt-node VM)                               │
│ ┌──────────────────────────────────────────────────────────┐    │
│ │  Mosquitto Broker (54.93.230.47:1883)                    │    │
│ │  Topics:                                                  │    │
│ │    • iot/data/{site_id}    (Uplink: Edge → Cloud)       │    │
│ │    • iot/control/{site_id} (Downlink: Cloud → Edge)     │    │
│ └──────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────┘
                            ▲
                            │ Aggregated Data + Commands
                            │
┌─────────────────────────────────────────────────────────────────┐
│ Layer 1: Edge (devices-node VM - K3s Cluster)                   │
│                                                                  │
│ Site: plant-a          Site: plant-b          Site: plant-c     │
│ ┌────────────┐        ┌────────────┐        ┌────────────┐     │
│ │ Publisher  │        │ Publisher  │        │ Publisher  │     │
│ │ (Sensor)   │        │ (Sensor)   │        │ (Sensor)   │     │
│ └─────┬──────┘        └─────┬──────┘        └─────┬──────┘     │
│       │ 50Hz                │ 50Hz                │ 50Hz        │
│       ▼                     ▼                     ▼             │
│ ┌─────────────────────────────────────────────────────────┐     │
│ │         Local MQTT Broker (edge-broker:1883)            │     │
│ │         Topic: iot/devices                              │     │
│ └─────────────────────────────────────────────────────────┘     │
│       │                     │                     │             │
│       ▼                     ▼                     ▼             │
│ ┌────────────┐        ┌────────────┐        ┌────────────┐     │
│ │ Edge Agent │        │ Edge Agent │        │ Edge Agent │     │
│ │ (plant-a)  │        │ (plant-b)  │        │ (plant-c)  │     │
│ │ Processing │        │ Processing │        │ Processing │     │
│ └────────────┘        └────────────┘        └────────────┘     │
└─────────────────────────────────────────────────────────────────┘
```

---

## Current System State

Based on the latest logs, here's what's happening **RIGHT NOW**:

### Active Components
✅ **3 Water Sensor Simulators** (Publishers)
- `plant-a`, `plant-b`, `plant-c`
- Each publishing at **50 Hz** (20ms interval)
- Simulating pressure, flow, tank levels, valve positions

✅ **3 Edge Agents** (K3s Pods)
- Each connected to **Local Broker** (input) and **Central Broker** (output)
- Currently in **DEBUG mode** (passthrough)
- Detecting leaks locally

✅ **1 Cloud Controller** (Docker Container)
- Running feedback loop every **10 seconds**
- Monitoring CPU: **15%** (IDLE state)
- Sending commands to edge agents

✅ **1 Cloud Subscriber** (Docker Container)
- Receiving data from all 3 sites
- Writing to InfluxDB
- Some errors on missing fields (non-critical)

### Current Problem
🔴 **Edge agents are stuck in DEBUG mode**, meaning they're sending **nearly all raw data** (50Hz) instead of aggregated data (1Hz). This defeats the purpose of edge processing.

**Why?** The controller keeps sending `{"mode": "DEBUG"}` during IDLE periods instead of `{"mode": "NORMAL"}`.

---

## Component Breakdown

### 1. Publisher (Sensor Simulator)
**File:** `publisher/device.py`  
**Location:** K3s Pod on `devices-node`

**What it does:**
```python
while True:
    # Generate realistic water system data
    pressure = 45 + random.gauss(0, 5)  # PSI
    flow = pressure * valve_position * 0.05  # GPM
    
    # Simulate occasional leaks (flow spike)
    if random.random() < 0.001:  # 0.1% chance
        flow += 30  # Sudden flow increase
    
    # Publish to Local MQTT
    mqtt.publish("iot/devices", json.dumps({
        "device_id": "pump-1",
        "pressure_psi": pressure,
        "flow_gpm": flow,
        "tank_level_pct": 75,
        "valve_position": valve_position,
        "timestamp": time.time()
    }))
    
    time.sleep(0.02)  # 50 Hz
```

**Key Points:**
- Publishes to **local broker only** (not aware of cloud)
- Generates ~180,000 messages per hour per device
- Simulates leaks randomly

---

### 2. Edge Agent (The "Brain")
**File:** `subscriber/edge_agent.py`  
**Location:** K3s Pod on `devices-node`

**Architecture:**
```
┌─────────────────────────────────────────────────┐
│              Edge Agent Pod                     │
│                                                 │
│  ┌──────────────────────────────────────────┐  │
│  │  Local MQTT Client (Subscriber)          │  │
│  │  - Subscribes to: iot/devices            │  │
│  │  - Callback: on_local_message()          │  │
│  └──────────────┬───────────────────────────┘  │
│                 │                               │
│                 ▼                               │
│  ┌──────────────────────────────────────────┐  │
│  │  Processing Logic                        │  │
│  │  1. Leak Detection (Immediate)           │  │
│  │  2. Mode Routing:                        │  │
│  │     - DEBUG → Send raw                   │  │
│  │     - NORMAL/ECONOMY → Buffer            │  │
│  └──────────────┬───────────────────────────┘  │
│                 │                               │
│                 ▼                               │
│  ┌──────────────────────────────────────────┐  │
│  │  Aggregation Thread (60s loop)           │  │
│  │  - Computes rolling averages             │  │
│  │  - Reduces 3000 msgs → 1 msg per min     │  │
│  └──────────────┬───────────────────────────┘  │
│                 │                               │
│                 ▼                               │
│  ┌──────────────────────────────────────────┐  │
│  │  Central MQTT Client (Publisher)         │  │
│  │  - Publishes to: iot/data/{site_id}      │  │
│  │  - Subscribes to: iot/control/{site_id}  │  │
│  │  - Callback: on_central_message()        │  │
│  └──────────────────────────────────────────┘  │
└─────────────────────────────────────────────────┘
```

**Processing Pipeline:**

#### Step 1: Leak Detection
```python
def detect_leak(record):
    pressure = float(record["pressure_psi"])
    flow = float(record["flow_gpm"])
    valve = float(record["valve_position"])
    
    # Physics-based anomaly detection
    expected_flow = pressure * valve * 0.05
    
    # If actual flow >> expected flow → LEAK!
    if valve > 10 and flow > (expected_flow + 15.0):
        return True
    return False
```

**Physics Behind It:**
- In a healthy pipe: Flow = Pressure × Valve × Constant
- In a leak: Flow increases while Pressure drops
- The agent detects this discrepancy **locally** without cloud

#### Step 2: Mode-Based Routing
```python
def on_local_message(client, userdata, msg):
    payload = json.loads(msg.payload)
    
    # CRITICAL: Always check for leaks first
    if detect_leak(payload):
        # IMMEDIATE ALERT (bypasses buffer)
        alert = payload.copy()
        alert["alert_type"] = "LEAK_DETECTED"
        central_client.publish("iot/data/site_a", json.dumps(alert), qos=1)
    
    # Then route based on mode
    if CURRENT_MODE == "DEBUG":
        # Send everything raw (50Hz)
        central_client.publish("iot/data/site_a", json.dumps(payload))
    else:
        # Buffer for aggregation (NORMAL/ECONOMY)
        data_buffer[payload["device_id"]].append(payload)
```

#### Step 3: Aggregation
```python
def aggregate_data(device_id, records):
    # Example: 3000 records in 60 seconds → 1 record
    count = len(records)
    avg_pressure = sum(r["pressure_psi"] for r in records) / count
    avg_flow = sum(r["flow_gpm"] for r in records) / count
    
    return {
        "device_id": device_id,
        "pressure_psi": round(avg_pressure, 2),
        "flow_gpm": round(avg_flow, 2),
        "aggregation_count": count,  # Metadata
        "mode": CURRENT_MODE
    }

# Background thread runs every 60 seconds
def aggregation_worker():
    while True:
        time.sleep(60)
        if CURRENT_MODE == "DEBUG":
            continue  # Skip aggregation
        
        # Process buffered data
        for device_id, records in data_buffer.items():
            agg = aggregate_data(device_id, records)
            central_client.publish("iot/data/site_a", json.dumps(agg))
```

**Bandwidth Reduction:**
- Input: 50 msgs/sec × 60 sec = **3,000 messages**
- Output: **1 aggregated message**
- **Compression: 99.97%**

---

### 3. Cloud Controller (The "Manager")
**File:** `cloud-node/controller.py`  
**Location:** Docker Container on `cloud-node`

**Control Loop (Every 10 seconds):**

```python
while True:
    # 1. Sense Infrastructure
    cpu_usage = 15.0  # Mock value
    
    # 2. Sense Application
    leaking_sites = query_influxdb_for_leaks()
    
    # 3. Decide Scenario
    if leaking_sites:
        scenario = "CRITICAL_LEAK"
    elif cpu_usage > 80:
        scenario = "CONGESTION"
    elif cpu_usage < 20:
        scenario = "IDLE"
    else:
        scenario = "NORMAL"
    
    # 4. Allocate Resources (Fairness Algorithm)
    for site in ["plant-a", "plant-b", "plant-c"]:
        if scenario == "CRITICAL_LEAK":
            if site in leaking_sites:
                command = {"mode": "DEBUG"}  # Full bandwidth
            else:
                command = {"mode": "ECONOMY"}  # Throttle
        elif scenario == "IDLE":
            command = {"mode": "DEBUG"}  # Utilize spare capacity
        else:
            command = {"mode": "NORMAL"}  # Default 1Hz
        
        # 5. Act: Send Command
        mqtt_client.publish(f"iot/control/{site}", json.dumps(command))
    
    time.sleep(10)
```

---

### 4. Cloud Subscriber (The "Pipe")
**File:** `subscriber/subscriber.py`  
**Location:** Docker Container on `cloud-node`

**Simple Pipeline:**
```python
def on_message(client, userdata, msg):
    # Receive from MQTT
    data = json.loads(msg.payload)
    
    # Write to InfluxDB
    point = {
        "measurement": "water_metrics",
        "tags": {
            "device_id": data["device_id"],
            "site_id": data["site_id"]
        },
        "fields": {
            "pressure_psi": data["pressure_psi"],
            "flow_gpm": data["flow_gpm"]
        },
        "time": data["timestamp"]
    }
    influx_client.write_points([point])
```

---

## Data Flow Walkthrough

### Scenario A: Normal Operation (NORMAL Mode)

```
Time: 0s
Publisher → Local Broker → Edge Agent
                            ├─ Leak Check: PASS
                            └─ Mode: NORMAL → Buffer

Time: 0.02s
Publisher → Local Broker → Edge Agent → Buffer (2 messages)

... (50 msgs/sec for 60 seconds) ...

Time: 60s
Edge Agent → Aggregation Thread
           ├─ Compute average of 3000 messages
           └─ Publish 1 message to Central Broker
                                    ↓
                          Cloud Subscriber → InfluxDB

Result: 3000 local messages → 1 cloud message (99.97% reduction)
```

### Scenario B: Leak Detected (Priority Override)

```
Time: 10.5s
Publisher generates leak (flow spike)
    ↓
Edge Agent detect_leak() → TRUE
    ├─ IMMEDIATE ALERT (QoS 1)
    │   └─ Central Broker → Cloud Subscriber → InfluxDB
    │
    └─ Also buffers the message (for aggregation)

Time: 10.6s
Controller queries InfluxDB
    ├─ Detects leak at plant-a
    └─ Publishes commands:
        • plant-a: {"mode": "DEBUG"}
        • plant-b: {"mode": "ECONOMY"}
        • plant-c: {"mode": "ECONOMY"}

Time: 10.7s
Edge Agent (plant-a) receives command
    ├─ Switches to DEBUG mode
    └─ Now sends ALL subsequent messages raw (50Hz)

Result: Leak site gets full bandwidth, others throttled
```

---

## Operating Modes

### Mode Comparison Table

| Mode | Edge Processing | Uplink Frequency | Bandwidth | Use Case |
|------|----------------|------------------|-----------|----------|
| **ECONOMY** | 60s aggregation | 1 msg/min | 0.3% | Cloud congestion |
| **NORMAL** | 60s aggregation | 1 msg/min | 2% | Default operation |
| **DEBUG** | Raw passthrough | 50 Hz | 100% | Leak diagnosis / Idle cloud |

---

## Controller Decision Logic

### Decision Matrix

| CPU | Leaks | Decision | plant-a | plant-b | plant-c |
|-----|-------|----------|---------|---------|---------|
| 15% | None | IDLE (Current) | DEBUG | DEBUG | DEBUG |
| 15% | None | **Should be:** | NORMAL | NORMAL | NORMAL |
| 50% | None | NORMAL | NORMAL | NORMAL | NORMAL |
| 85% | None | CONGESTION | ECONOMY | ECONOMY | ECONOMY |
| 50% | plant-b | LEAK | ECONOMY | DEBUG | ECONOMY |

---

## Performance Metrics

### Bandwidth Reduction (Theoretical)

**Baseline (No Edge):**
- 3 sites × 50 Hz × 500 bytes/msg = **75 KB/sec**
- Per day: **6.48 GB/day**

**Dynamic Edge (NORMAL mode):**
- 3 sites × 1 msg/min × 500 bytes = **25 bytes/sec**
- Per day: **2.16 MB/day**
- **Reduction: 99.97%**

### Current Actual Performance (DEBUG mode bug)

**What you're seeing now:**
- All sites in DEBUG mode
- ~150 msg/sec to cloud
- **0% bandwidth reduction** (system not working as intended)

---

## Troubleshooting

### Problem: No Data in InfluxDB
**Check:**
```powershell
ssh devices "sudo kubectl logs -n iot-edge -l app=publisher --tail 20"
ssh devices "sudo kubectl logs -n iot-edge -l app=edge-agent --tail 50"
ssh cloud "docker logs cloud-node-subscriber-1 --tail 50"
```

### Problem: Edge Agents Stuck in Wrong Mode
**Fix:**
```powershell
ssh devices "sudo kubectl rollout restart deployment/edge-agent -n iot-edge"
```

### Problem: Controller Not Sending Commands
**Check:**
```powershell
ssh cloud "docker logs cloud-node-controller-1 --tail 100"
```

---

## Next Steps

### Fix Controller Logic
Update `cloud-node/controller.py` to send `NORMAL` instead of `DEBUG` during IDLE periods.

### Run Experiments
1. Baseline bandwidth measurement
2. Dynamic Edge bandwidth measurement
3. Leak response time testing
4. Fairness algorithm validation
