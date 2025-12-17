# Feedback Loop - Complete Technical Explanation

**Document Version**: 1.0  
**Last Updated**: December 15, 2025

---

## Overview

This document explains how the **Dynamic Edge Feedback Loop** works end-to-end, from CPU measurement to edge behavior change. The feedback loop enables the cloud to dynamically control edge node behavior based on real-time system state (CPU load and leak detection).

---

## Step 1: Prometheus Scrapes Metrics (Continuous)

**Frequency**: Every 5 seconds

```
Prometheus → HTTP GET → Cloud Node Exporter (172.31.33.61:9100)
                       ↓
                Gets: node_cpu_seconds_total{mode="idle"} = 123456.78
                                            {mode="user"} = 45678.90
                                            {mode="system"} = 12345.67
                       ↓
                Stores in time-series database
```

**Configuration**: `monitoring-node/prometheus/prometheus.yml`
```yaml
scrape_configs:
  - job_name: 'cloud_node_exporter'
    static_configs:
      - targets: ['172.31.33.61:9100']
    scrape_interval: 5s
```

**What Gets Scraped**:
- CPU counters (cumulative seconds spent in each mode)
- Memory usage
- Disk I/O
- Network traffic

---

## Step 2: Controller Queries Prometheus (Every 10 seconds)

**File**: `cloud-node/controller.py`

### Main Control Loop

```python
def main():
    while True:
        # 1. Gather system state
        site_states, cloud_cpu = check_system_state()
        
        # 2. Decide modes based on fairness algorithm
        commands = enforce_fairness(site_states, cloud_cpu)
        
        # 3. Send commands to edge nodes
        send_commands(commands)
        
        time.sleep(10)  # Control loop runs every 10 seconds
```

### Querying Prometheus

**Function**: `check_system_state()`

```python
# Construct PromQL query
query = '100 - (avg(rate(node_cpu_seconds_total{instance="172.31.33.61:9100",mode="idle"}[1m])) * 100)'

# Query Prometheus HTTP API
response = requests.get(
    "http://172.31.42.61:9090/api/v1/query",
    params={'query': query},
    timeout=5
)

# Parse JSON response
if response.status_code == 200:
    data = response.json()
    cloud_cpu_usage = float(data['data']['result'][0]['value'][1])
```

**Response Format**:
```json
{
  "status": "success",
  "data": {
    "resultType": "vector",
    "result": [
      {
        "metric": {},
        "value": [1734278400, "23.45"]
      }
    ]
  }
}
```
- `value[0]`: Unix timestamp
- `value[1]`: CPU usage percentage (as string)

**PromQL Query Breakdown**:
```
100 - (avg(rate(node_cpu_seconds_total{instance="172.31.33.61:9100",mode="idle"}[1m])) * 100)
│     │   │    │                         │                                  │      │    │
│     │   │    │                         │                                  │      │    └─ Convert to percentage
│     │   │    │                         │                                  │      └─ Calculate rate over 1 minute
│     │   │    │                         │                                  └─ Filter for idle mode
│     │   │    │                         └─ Filter for specific node exporter
│     │   │    └─ Metric name (cumulative counter)
│     │   └─ Average across all CPU cores
│     └─ Invert (convert idle% to busy%)
└─ Final result: CPU usage percentage
```

**Example Calculation**:
```
node_cpu_seconds_total{mode="idle"} at T=0  = 950,000 seconds
node_cpu_seconds_total{mode="idle"} at T=60 = 950,057 seconds

Rate = (950,057 - 950,000) / 60 = 0.95 seconds/second
      = 95% idle

CPU Usage = 100 - (0.95 × 100) = 5%
```

---

## Step 3: Controller Makes Decision

**File**: `cloud-node/controller.py`

### Fairness Algorithm

```python
def enforce_fairness(site_states, cloud_cpu):
    """
    4-tier priority-based decision system
    """
    commands = {}
    leaking_sites = [s for s, state in site_states.items() if state == "LEAK_DETECTED"]
    
    # PRIORITY 1: CRITICAL EVENT (Leak)
    # Safety overrides all other concerns
    if leaking_sites:
        log.info(f"SCENARIO: CRITICAL LEAK DETECTED. Prioritizing {leaking_sites}")
        for site in SITES:
            if site in leaking_sites:
                commands[site] = "DEBUG"   # Full resolution for leak analysis
            else:
                commands[site] = "ECONOMY" # Throttle others to save bandwidth
    
    # PRIORITY 2: CLOUD CONGESTION (High CPU)
    # Protect infrastructure from overload
    elif cloud_cpu > 80.0:
        log.info(f"SCENARIO: CLOUD CONGESTION (CPU {cloud_cpu}%). Throttling all sites.")
        for site in SITES:
            commands[site] = "ECONOMY"  # All sites throttle equally (fairness)
    
    # PRIORITY 3: CLOUD IDLE (Resource Maximization)
    # Utilize spare capacity for better analytics
    elif cloud_cpu < 20.0:
        log.info(f"SCENARIO: CLOUD IDLE (CPU {cloud_cpu}%). Requesting High-Fidelity Data.")
        for site in SITES:
            commands[site] = "DEBUG"  # Request high-res data from everyone
    
    # PRIORITY 4: NORMAL OPERATION
    # Standard balanced operation
    else:
        log.info(f"SCENARIO: NORMAL OPERATION (CPU {cloud_cpu}%).")
        for site in SITES:
            commands[site] = "NORMAL"  # Standard 60s aggregation
    
    return commands
```

### Decision Matrix

| Condition | Cloud CPU | Commands | Behavior |
|-----------|-----------|----------|----------|
| **Leak Detected** | Any | Leaking: DEBUG<br>Others: ECONOMY | Safety prioritization |
| **Congestion** | > 80% | All: ECONOMY | Load shedding |
| **Idle** | < 20% | All: DEBUG | Resource maximization |
| **Normal** | 20-80% | All: NORMAL | Standard operation |

### Example Scenarios

**Scenario A**: CPU = 15%, No leaks
```python
commands = {
    "plant-a": "DEBUG",   # Send all raw data
    "plant-b": "DEBUG",   # Send all raw data
    "plant-c": "DEBUG"    # Send all raw data
}
```

**Scenario B**: CPU = 85%, No leaks
```python
commands = {
    "plant-a": "ECONOMY",  # Throttle to 5min
    "plant-b": "ECONOMY",  # Throttle to 5min
    "plant-c": "ECONOMY"   # Throttle to 5min
}
```

**Scenario C**: CPU = 45%, Leak at plant-b
```python
commands = {
    "plant-a": "ECONOMY",  # Throttle to save bandwidth
    "plant-b": "DEBUG",    # Full resolution for leak
    "plant-c": "ECONOMY"   # Throttle to save bandwidth
}
```

---

## Step 4: Controller Publishes MQTT Commands

**File**: `cloud-node/controller.py`

### Publishing Commands

```python
def send_commands(commands):
    """
    Publishes mode commands to MQTT control topics
    """
    for site, mode in commands.items():
        topic = f"iot/control/{site}"
        payload = json.dumps({
            "mode": mode,
            "timestamp": time.time()
        })
        
        mqtt_client.publish(topic, payload, qos=1)
        # qos=1 ensures at-least-once delivery (important for control messages)
```

### MQTT Message Flow

```
Controller Container (172.31.33.61)
         │
         │ TCP Connection
         ↓
Central MQTT Broker (54.93.230.47)
         │
         │ Topic: iot/control/plant-a
         │ Payload: {"mode": "NORMAL", "timestamp": 1734278400.123}
         │
         ↓
Edge Agent (K3s pod on devices node)
```

### Topic Structure

| Topic | Publisher | Subscriber | Purpose |
|-------|-----------|------------|---------|
| `iot/control/plant-a` | Controller | Edge Agent (plant-a) | Commands for site A |
| `iot/control/plant-b` | Controller | Edge Agent (plant-b) | Commands for site B |
| `iot/control/plant-c` | Controller | Edge Agent (plant-c) | Commands for site C |

### Message Format

```json
{
  "mode": "NORMAL",
  "timestamp": 1734278400.123
}
```

**Fields**:
- `mode`: One of ["DEBUG", "NORMAL", "ECONOMY"]
- `timestamp`: Unix timestamp (for latency measurement)

---

## Step 5: Edge Agent Receives Command

**File**: `subscriber/edge_agent.py`

### Subscription Setup

```python
# On startup, edge agent subscribes to its control topic
central_client = mqtt.Client(client_id=f"edge_uplink_{SITE_ID}")
central_client.on_message = on_central_message
central_client.connect(CENTRAL_BROKER, CENTRAL_PORT, 60)
central_client.subscribe(f"{CENTRAL_TOPIC_CONTROL}/{SITE_ID}")
central_client.loop_start()

# Example: plant-a subscribes to "iot/control/plant-a"
```

### Message Handler

```python
def on_central_message(client, userdata, msg):
    """
    Callback when control message arrives
    """
    global CURRENT_MODE
    
    try:
        # Parse JSON payload
        cmd = json.loads(msg.payload.decode())
        new_mode = cmd.get("mode")
        
        # Validate mode
        if new_mode in ["NORMAL", "DEBUG", "ECONOMY"]:
            log.info(f"Received Command: Change Mode {CURRENT_MODE} → {new_mode}")
            
            # Update global state variable
            CURRENT_MODE = new_mode
            
            # Update Prometheus metric for observability
            mode_values = {"ECONOMY": 0, "NORMAL": 1, "DEBUG": 2}
            g_mode.set(mode_values[new_mode])
        else:
            log.warning(f"Invalid mode received: {new_mode}")
            
    except Exception as e:
        log.error(f"Error processing command: {e}")
```

### State Change Impact

**Before**:
- `CURRENT_MODE` = "DEBUG"
- `edge_operating_mode` metric = 2

**Command Received**: `{"mode": "NORMAL"}`

**After**:
- `CURRENT_MODE` = "NORMAL"
- `edge_operating_mode` metric = 1

---

## Step 6: Edge Agent Changes Behavior

**File**: `subscriber/edge_agent.py`

### Ingestion Handler

```python
def on_local_message(client, userdata, msg):
    """
    Processes incoming sensor data from local publishers
    """
    global CURRENT_MODE
    m_ingress.inc()  # Prometheus counter
    
    try:
        payload = json.loads(msg.payload.decode())
        # Example: {"device_id": "sensor_1", "pressure_psi": 120, "flow_gpm": 150, ...}
        
        # PRIORITY 1: Leak Detection (Always Immediate)
        if detect_leak(payload):
            log.warning(f"LEAK DETECTED on {payload.get('device_id')}! Sending ALERT.")
            m_leaks.inc()
            
            # Bypass all aggregation - send immediately
            alert_payload = payload.copy()
            alert_payload["alert_type"] = "LEAK_DETECTED"
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(alert_payload), qos=1)
            return  # Exit early
        
        # PRIORITY 2: Mode-Based Routing
        if CURRENT_MODE == "DEBUG":
            # Passthrough mode: Send everything raw
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(payload))
            m_egress.inc()
        else:
            # NORMAL or ECONOMY: Buffer for aggregation
            with buffer_lock:
                data_buffer[payload.get("device_id")].append(payload)
                
    except Exception as e:
        log.error(f"Error processing local msg: {e}")
```

### Aggregation Worker Thread

```python
def aggregation_worker():
    """
    Background thread that periodically aggregates buffered data
    """
    while True:
        # Determine sleep time based on current mode
        if CURRENT_MODE == "DEBUG":
            continue  # No aggregation in debug mode
        elif CURRENT_MODE == "NORMAL":
            time.sleep(60)  # 60 seconds
        elif CURRENT_MODE == "ECONOMY":
            time.sleep(300)  # 5 minutes
        
        log.info("Running aggregation cycle...")
        
        # Take snapshot of buffer and clear it
        with buffer_lock:
            snapshot = data_buffer.copy()
            data_buffer.clear()
        
        # Process each device's data
        for device_id, records in snapshot.items():
            if not records:
                continue
            
            # Calculate averages
            agg_record = aggregate_data(device_id, records)
            
            # Send aggregated record to cloud
            if agg_record:
                central_client.publish(
                    f"{CENTRAL_TOPIC_DATA}/{SITE_ID}",
                    json.dumps(agg_record)
                )
                m_egress.inc()
```

### Aggregation Function

```python
def aggregate_data(device_id, records):
    """
    Compresses a list of records into a single average
    """
    if not records:
        return None
    
    count = len(records)
    
    # Calculate averages
    avg_pressure = sum(r["pressure_psi"] for r in records) / count
    avg_flow = sum(r["flow_gpm"] for r in records) / count
    avg_level = sum(r["tank_level_pct"] for r in records) / count
    
    # Use timestamp of last record
    last_ts = records[-1]["timestamp"]
    
    return {
        "device_id": device_id,
        "site_id": SITE_ID,
        "timestamp": last_ts,
        "pressure_psi": round(avg_pressure, 2),
        "flow_gpm": round(avg_flow, 2),
        "tank_level_pct": round(avg_level, 2),
        "aggregation_count": count,
        "mode": CURRENT_MODE
    }
```

---

## Step 7: Behavior Change Impact

### Data Flow Comparison

**DEBUG Mode** (Passthrough):
```
Publishers (50 Hz) → Edge Agent → Central Broker → Cloud
                       └─ No buffering
                       └─ 1,500 msg/sec sent to cloud
```

**NORMAL Mode** (60s aggregation):
```
Publishers (50 Hz) → Edge Agent → [Buffer 60s] → Central Broker → Cloud
                       └─ Collects 3,000 msgs
                       └─ Sends 1 avg msg/min
                       └─ 30 msg/min total (98% reduction)
```

**ECONOMY Mode** (5min aggregation):
```
Publishers (50 Hz) → Edge Agent → [Buffer 5min] → Central Broker → Cloud
                       └─ Collects 15,000 msgs
                       └─ Sends 1 avg msg/5min
                       └─ 6 msg/min total (99.6% reduction)
```

### Bandwidth Impact

| Mode | Aggregation Window | Messages to Cloud | Bandwidth Saved |
|------|-------------------|-------------------|-----------------|
| DEBUG | None | 1,500 msg/sec | 0% (baseline) |
| NORMAL | 60 seconds | 30 msg/min | 98.0% |
| ECONOMY | 5 minutes | 6 msg/min | 99.6% |

**Calculation** (for 30 devices):
- Raw rate: 30 devices × 50 Hz = 1,500 msg/sec = 90,000 msg/min
- NORMAL: 30 devices × 1 msg/min = 30 msg/min
- Reduction: (90,000 - 30) / 90,000 = 99.97% → **~98%** (accounting for leak alerts)

---

## Complete Timeline Example

### Scenario: Cloud Load Spike and Recovery

```
T=0s:   Prometheus scrapes cloud node
        node_cpu_seconds_total{mode="idle"} shows 95% idle
        → Calculated CPU usage = 5%

T=10s:  Controller queries Prometheus
        GET http://172.31.42.61:9090/api/v1/query?query=...
        Response: {"value": [1734278400, "5.0"]}
        
        Decision: CPU < 20% → "CLOUD IDLE" scenario
        Publishes: {"mode": "DEBUG"} to iot/control/plant-{a,b,c}

T=11s:  Edge Agents receive MQTT messages
        on_central_message() callback fires
        CURRENT_MODE: NORMAL → DEBUG
        edge_operating_mode metric: 1 → 2

T=12s:  Edge Agents switch to passthrough mode
        on_local_message() now sends all data immediately
        Cloud ingestion rate: 30 msg/min → 1,500 msg/sec

T=15s:  Cloud CPU begins rising (InfluxDB ingestion load)
        Prometheus scrapes: idle dropping

T=60s:  Heavy data load causes CPU spike
        Prometheus scrapes: 15% idle
        → Calculated CPU usage = 85%

T=70s:  Controller queries Prometheus
        Response: {"value": [1734278460, "85.0"]}
        
        Decision: CPU > 80% → "CLOUD CONGESTION" scenario
        Publishes: {"mode": "ECONOMY"} to all sites

T=71s:  Edge Agents receive throttle command
        CURRENT_MODE: DEBUG → ECONOMY
        edge_operating_mode metric: 2 → 0

T=72s:  Edge Agents switch to 5-min aggregation
        aggregation_worker() now sleeps 300 seconds
        Cloud ingestion rate: 1,500 msg/sec → 6 msg/min

T=75s:  Cloud CPU begins dropping (reduced load)

T=300s: System stabilizes
        Prometheus scrapes: 55% idle
        → Calculated CPU usage = 45%

T=310s: Controller queries Prometheus
        Response: {"value": [1734278700, "45.0"]}
        
        Decision: 20% < CPU < 80% → "NORMAL" scenario
        Publishes: {"mode": "NORMAL"} to all sites

T=311s: Edge Agents receive normal command
        CURRENT_MODE: ECONOMY → NORMAL
        edge_operating_mode metric: 0 → 1

T=312s: Edge Agents switch to 60s aggregation
        Cloud ingestion rate: 6 msg/min → 30 msg/min

T=∞:    System maintains steady state at ~45% CPU
```

---

## The Closed Feedback Loop

### System Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    FEEDBACK LOOP                             │
│                                                              │
│  ┌──────────┐                                               │
│  │  Cloud   │  High CPU (85%)                               │
│  │ InfluxDB │────────────────┐                              │
│  └──────────┘                │                              │
│       ↑                      │                              │
│       │                      ↓                              │
│       │              ┌──────────────┐                       │
│       │              │ Prometheus   │                       │
│       │              │ (Metrics DB) │                       │
│       │              └──────┬───────┘                       │
│       │                     │                               │
│       │                     │ HTTP GET /api/v1/query        │
│       │                     │ CPU = 85%                     │
│       │                     ↓                               │
│       │              ┌──────────────┐                       │
│       │              │  Controller  │                       │
│       │              │   (Brain)    │                       │
│       │              └──────┬───────┘                       │
│       │                     │                               │
│       │                     │ MQTT Publish                  │
│       │                     │ iot/control/plant-a           │
│       │                     │ {"mode": "ECONOMY"}           │
│       │                     ↓                               │
│       │              ┌──────────────┐                       │
│       │              │  Edge Agent  │                       │
│       │              │ (plant-a)    │                       │
│       │              └──────┬───────┘                       │
│       │                     │                               │
│       │                     │ Reduces data rate             │
│       │                     │ 1,500 msg/s → 6 msg/min       │
│       │                     ↓                               │
│       │  Reduced Load ┌──────────────┐                     │
│       └───────────────│ Central MQTT │                     │
│        (CPU drops)    │    Broker    │                     │
│                       └──────────────┘                     │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

### Negative Feedback Characteristics

**Problem**: High CPU load
**Detection**: Prometheus monitors CPU
**Decision**: Controller sees CPU > 80%
**Action**: Send ECONOMY command
**Effect**: Edge nodes throttle data
**Result**: CPU load decreases
**Stability**: System returns to 20-80% range

This is a **negative feedback loop** that provides **self-regulation** and **stability**.

---

## Performance Characteristics

### Control Loop Latency

| Stage | Latency | Cumulative |
|-------|---------|------------|
| CPU spike occurs | 0s | 0s |
| Prometheus scrapes (worst case) | 0-5s | 5s |
| Controller queries (worst case) | 0-10s | 15s |
| Controller decides | <0.1s | 15s |
| MQTT publishes | ~0.5s | 15.5s |
| Edge receives | ~0.5s | 16s |
| Edge changes mode | <0.1s | 16s |
| **Total reaction time** | - | **~16 seconds** |

### Bandwidth Efficiency

**Scenario**: 30 devices, 50 Hz, 200 bytes/msg

| Mode | Messages/min | Bytes/sec | Daily Volume |
|------|-------------|-----------|--------------|
| DEBUG | 90,000 | 300 KB/s | 25.9 GB |
| NORMAL | 30 | 100 bytes/s | 8.6 MB |
| ECONOMY | 6 | 20 bytes/s | 1.7 MB |

**Bandwidth Savings** (NORMAL vs DEBUG): **99.97%**

---

## Safety Guarantees

### Leak Detection Override

**Critical**: Leak detection **always bypasses** aggregation mode

```python
if detect_leak(payload):
    central_client.publish(alert, qos=1)
    return  # Exit before mode check
```

**This ensures**:
- Leaks are detected in <1 second (local processing)
- Alerts are sent regardless of mode (ECONOMY can't block safety)
- High-priority QoS (qos=1) guarantees delivery

### Controller Priority

Priority order (highest to lowest):
1. **Leak Detection** → Override all CPU considerations
2. **Cloud Congestion** → Protect infrastructure
3. **Cloud Idle** → Maximize analytics
4. **Normal Operation** → Default state

---

## Observability

### Metrics Exposed

**Controller**: (not yet implemented, but should expose)
- `controller_decisions_total{scenario}`
- `controller_cpu_reading_seconds`
- `controller_prometheus_query_errors_total`

**Edge Agent**: `http://edge-pod:8000/metrics`
- `edge_operating_mode` (0=ECONOMY, 1=NORMAL, 2=DEBUG)
- `edge_ingress_messages_total`
- `edge_egress_messages_total`
- `edge_leaks_detected_total`

**Prometheus**: `http://172.31.42.61:9090`
- `node_cpu_seconds_total{instance="172.31.33.61:9100"}`
- All other infrastructure metrics

---

## Configuration

### Environment Variables

**Controller** (`docker-compose.dynamic.yml`):
```yaml
environment:
  BROKER: "54.93.230.47"
  INFLUXDB_URL: "http://influxdb:8086"
  PROMETHEUS_URL: "http://172.31.42.61:9090"  # Optional override
```

**Edge Agent** (K3s manifest):
```yaml
env:
  - name: SITE_ID
    value: "plant-a"
  - name: LOCAL_BROKER
    value: "local-broker"
  - name: CENTRAL_BROKER
    value: "54.93.230.47"
  - name: AGGREGATION_WINDOW
    value: "60"  # Default for NORMAL mode
```

---

## Troubleshooting

### Controller Not Responding

**Check**:
```bash
ssh cloud "docker logs cloud-node-controller-1 --tail 50"
```

**Common Issues**:
- Prometheus unreachable → Falls back to default CPU (15%)
- InfluxDB query timeout → No leak detection
- MQTT connection failed → No commands sent

### Edge Agent Not Changing Mode

**Check**:
```bash
ssh devices "sudo kubectl logs -n iot-edge deployment/edge-agent-site-a --tail 50"
```

**Common Issues**:
- Not subscribed to control topic
- JSON parse error in command
- `CURRENT_MODE` variable not updating

### Feedback Loop Not Stabilizing

**Symptoms**: CPU oscillates between 10% and 90%

**Causes**:
- Control loop interval too short (increase from 10s to 30s)
- Thresholds too close (widen 20%/80% gap)
- Aggregation window too short (increase NORMAL from 60s to 120s)

---

## Future Enhancements

### Adaptive Thresholds
Instead of fixed 20%/80%, learn optimal thresholds from historical data.

### Per-Site Bandwidth Allocation
Fine-grained control: "Plant-A gets 50% of bandwidth, Plant-B gets 30%, Plant-C gets 20%"

### Predictive Control
Use time-series forecasting to predict CPU spikes and pre-emptively throttle.

### Multi-Metric Decision
Consider memory, disk I/O, network bandwidth alongside CPU.

---

**End of Document**
