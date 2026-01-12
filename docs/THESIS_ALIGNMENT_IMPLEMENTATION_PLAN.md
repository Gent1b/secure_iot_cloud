# THESIS ALIGNMENT - Implementation Plan

## Executive Summary
This document details the concrete code changes required to ensure the IoT system produces **valid, reproducible, and defensible thesis data**. All changes are surgical fixes to existing components—NO architectural redesign.

---

## MANDATORY FIX #1: Ground-Truth Leak Tracking ✅
**Status:** ALREADY IMPLEMENTED
- `device.py` line 129: `leak_flag` field already sent in telemetry
- This provides timestamped ground truth for all scenarios
- **No changes required**

---

## MANDATORY FIX #2: Leak Detection Responsibility Separation
**Problem:** Cloud subscriber re-detects leaks even when edge has already done it.

### Changes Required:

#### File: `subscriber/subscriber.py` (Lines 118-142)
**Current Behavior:**
```python
# Cloud performs its own leak detection
is_cloud_detected_leak = False
expected_flow = pressure * valve * 0.05
if valve > 10 and flow > (expected_flow + 15.0):
    is_cloud_detected_leak = True
    leaks_detected.inc()
    
final_leak_flag = int(data.get("leak_flag", is_cloud_detected_leak))
```

**Required Behavior:**
```python
# THESIS FIX: Cloud trusts edge-provided leak_flag
# Only baseline/centralized scenario should detect leaks in cloud
# Edge scenarios provide pre-computed leak_flag

MODE = os.getenv("DEPLOYMENT_MODE", "centralized")  # centralized|edge

if MODE == "centralized":
    # Cloud performs leak detection (Baseline scenario)
    expected_flow = pressure * valve * 0.05
    if valve > 10 and flow > (expected_flow + 15.0):
        leaks_detected.inc()
        final_leak_flag = 1
    else:
        final_leak_flag = 0
else:
    # Trust edge-provided leak_flag (Static/Dynamic scenarios)
    final_leak_flag = int(data.get("leak_flag", 0))
    if final_leak_flag == 1:
        leaks_detected.inc()  # Count for metrics only
```

**Why:** Ensures each scenario has CLEAR detection responsibility.

---

## MANDATORY FIX #3: Deterministic Experiment Reproducibility
**Problem:** Random leak timing makes cross-scenario comparison invalid.

### Changes Required:

#### File: `publisher/device.py` (Lines 107-114)
**Current Behavior:**
```python
# Random Event: Leak Injection (Rare)
if not self.leak_active and random.random() < 0.001:
    self.leak_active = True
    log.warning("[%s] LEAK STARTED!", self.device_id)
    
if self.leak_active and random.random() < 0.05:
    self.leak_active = False
    log.info("[%s] Leak repaired.", self.device_id)
```

**Required Behavior:**
```python
# THESIS FIX: Deterministic leak schedule
# Set via environment variable or fixed seed
LEAK_SCHEDULE_SEED = int(os.getenv("LEAK_SEED", 42))
random.seed(LEAK_SCHEDULE_SEED)

# Use deterministic timeline instead of random
# Option 1: Fixed seed (simple)
# Option 2: Predefined schedule from file (better)
LEAK_TIMELINE = os.getenv("LEAK_TIMELINE", "")  # Format: "device_id:start_step:duration,..."

# Inject leak at specific simulation step
if LEAK_TIMELINE:
    # Parse and check if this device should leak now
    pass
else:
    # Fallback to seeded random (at least reproducible)
    if not self.leak_active and random.random() < 0.001:
        self.leak_active = True
```

**Why:** All scenarios experience IDENTICAL leak events.

---

## MANDATORY FIX #4: Input Rate Control (Baseline Validity)
**Problem:** `PUBLISH_INTERVAL = 1.0` creates unbounded publishing rate.

### Changes Required:

#### File: `publisher/device.py` (Line 26, Line 181)
**Current:**
```python
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", 1.0))
# ...
time.sleep(PUBLISH_INTERVAL)
```

**Required:**
```python
TARGET_RATE_HZ = int(os.getenv("TARGET_RATE_HZ", 50))  # 50Hz = 0.02s
PUBLISH_INTERVAL = 1.0 / TARGET_RATE_HZ

# THESIS FIX: Enforce real-time throttling
# Use monotonic clock to prevent drift
next_publish_time = time.monotonic()

while True:
    station.step()
    data = station.get_telemetry()
    
    # ... publish ...
    
    # Wait until next scheduled time slot
    next_publish_time += PUBLISH_INTERVAL
    sleep_duration = next_publish_time - time.monotonic()
    if sleep_duration > 0:
        time.sleep(sleep_duration)
```

**Why:** Baseline represents realistic centralized system, not stress test.

---

## MANDATORY FIX #5: Feedback Loop Stability
**Problem:** Controller switches modes on instantaneous thresholds (oscillation risk).

### Changes Required:

#### File: `cloud-node/controller.py` (Lines 147-162)
**Current:**
```python
elif cloud_cpu > 80.0:
    # Switch to ECONOMY immediately
    for site in SITES:
        commands[site] = "ECONOMY"
```

**Required:**
```python
# THESIS FIX: Add hysteresis and cooldown
LAST_MODE_CHANGE = {}  # site -> timestamp
COOLDOWN_SECONDS = 30

# State tracking
CURRENT_MODES = {site: "NORMAL" for site in SITES}
CPU_HISTORY = deque(maxlen=3)  # 3 samples = 30 seconds

def enforce_fairness_with_stability(site_states, cloud_cpu):
    CPU_HISTORY.append(cloud_cpu)
    avg_cpu = sum(CPU_HISTORY) / len(CPU_HISTORY)
    
    # ECONOMY: Enter at 80%, Exit at 60% (hysteresis)
    # DEBUG: Sustain for minimum 60s after leak clears
    
    for site in SITES:
        current_mode = CURRENT_MODES[site]
        last_change = LAST_MODE_CHANGE.get(site, 0)
        
        # Cooldown check
        if time.time() - last_change < COOLDOWN_SECONDS:
            commands[site] = current_mode  # Keep current
            continue
        
        # Apply decision logic with hysteresis...
        if site_states[site] == "LEAK_DETECTED":
            new_mode = "DEBUG"
        elif avg_cpu > 80 and current_mode != "ECONOMY":
            new_mode = "ECONOMY"
        elif avg_cpu < 60 and current_mode == "ECONOMY":
            new_mode = "NORMAL"
        else:
            new_mode = current_mode
        
        if new_mode != current_mode:
            LAST_MODE_CHANGE[site] = time.time()
            CURRENT_MODES[site] = new_mode
        
        commands[site] = new_mode
```

**Why:** Mode changes reflect sustained conditions, not noise.

---

## MANDATORY FIX #6: Metrics Completeness (Edge Visibility)
**Status:** ALREADY IMPLEMENTED
- `edge_agent.py` line 44-46: Metrics already exposed on port 8000
- `g_mode` gauge tracks operating mode
- `m_leaks` counter tracks detections
- **No changes required** (verify Prometheus scrapes them)

---

## MANDATORY FIX #7: Mode Transition Traceability
**Problem:** Mode switches not explicitly persisted as events.

### Changes Required:

#### File: `subscriber/edge_agent.py` (Add after line 34)
**Add New Metric:**
```python
m_mode_changes = Counter("edge_mode_transitions_total", "Mode changes", ["from_mode", "to_mode"])
```

#### File: `subscriber/edge_agent.py` (In command handler)
**Log Every Transition:**
```python
def on_message_control(client, userdata, msg):
    global CURRENT_MODE
    try:
        command = json.loads(msg.payload.decode())
        new_mode = command.get("mode", "NORMAL")
        
        # THESIS FIX: Trace transition
        if new_mode != CURRENT_MODE:
            m_mode_changes.labels(from_mode=CURRENT_MODE, to_mode=new_mode).inc()
            log.info(f"MODE TRANSITION: {CURRENT_MODE} → {new_mode} (Command: {command})")
            CURRENT_MODE = new_mode
    except Exception as e:
        log.error(f"Control message error: {e}")
```

#### File: `cloud-node/controller.py` (Add to send_commands)
**Persist Decision Events to InfluxDB:**
```python
def send_commands(commands):
    for site, mode in commands.items():
        # Publish MQTT command
        topic = f"{TOPIC_CONTROL}/{site}"
        payload = json.dumps({"mode": mode, "timestamp": time.time()})
        mqtt_client.publish(topic, payload, qos=1)
        
        # THESIS FIX: Persist decision as event
        point = Point("controller_decisions") \
            .tag("site_id", site) \
            .field("target_mode", mode) \
            .field("decision_time", time.time())
        try:
            query_api.write(bucket=INFLUX_BUCKET, record=point)
        except Exception as e:
            log.error(f"Failed to persist decision: {e}")
```

**Why:** Feedback loop timeline is reconstructible.

---

## Implementation Priority

### Phase 1: CRITICAL (Do First)
1. ✅ Fix #2: Detection Responsibility Separation
2. ✅ Fix #4: Rate Control (Baseline Validity)
3. ✅ Fix #5: Controller Stability

### Phase 2: REPRODUCIBILITY
4. ✅ Fix #3: Deterministic Leak Schedule

### Phase 3: OBSERVABILITY
5. ✅ Fix #7: Mode Transition Tracing

### Already Valid
- Fix #1: Ground truth ✅
- Fix #6: Edge metrics ✅

---

## Validation Tests

After implementation, verify:

1. **Detection Separation:**
   ```bash
   # Centralized: Cloud should increment leaks_detected
   # Edge: Cloud should NOT increment (trusts edge)
   ```

2. **Rate Control:**
   ```bash
   # All scenarios publish at exact 50Hz (not variable)
   ```

3. **Determinism:**
   ```bash
   # Run same scenario twice with same LEAK_SEED → identical results
   ```

4. **Stability:**
   ```bash
   # Controller shouldn't flip-flop modes every 10s
   ```

5. **Traceability:**
   ```bash
   # Query InfluxDB/Prometheus for full timeline of mode transitions
   ```

---

## Thesis Defense Ready

After fixes, you can prove:
- ✅ **Centralized vs Edge bandwidth:** Measured with identical input
- ✅ **Detection latency:** Ground truth vs cloud vs edge timestamps
- ✅ **Feedback loop:** Sense → Decide → Act → Effect (all logged)
- ✅ **Adaptive behavior:** CPU-aware throttling with stable transitions
- ✅ **Reproducible experiments:** Same seed = same results

No feature creep. No redesign. Just alignment.
