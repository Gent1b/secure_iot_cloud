# Data Flow & Message Specification

This document provides the technical specification for the data exchanged within the system. Understanding the schema is critical to explaining how the system achieves "Semantic Compression" (aggregating data without losing meaning).

## 1. Topic Taxonomy (MQTT)
Messages are organized into a strict hierarchy to allow granular subscription and security controls.

### Local Plane (Edge Only)
*   `iot/devices`:
    *   **Publisher**: Python Sensors
    *   **Subscriber**: Edge Agent
    *   **Content**: Raw 50Hz physics data.
    *   **Visibility**: Hidden from Cloud.

### Uplink Plane (Edge → Transport → Cloud)
*   `iot/data/{site_id}`:
    *   **Publisher**: Edge Agent
    *   **Subscriber**: Cloud Subscriber
    *   **Content**: Aggregated telemetry (1Hz) OR Raw passthrough (50Hz), depending on mode.
    *   **Example**: `iot/data/plant-a`

### Downlink Plane (Cloud → Transport → Edge)
*   `iot/control/{site_id}`:
    *   **Publisher**: Cloud Controller
    *   **Subscriber**: Edge Agent
    *   **Content**: Configuration commands (Mode changes).
    *   **Example**: `iot/control/plant-a`

---

## 2. Telemetry Schema

### Raw Sensor Message (JSON)
Generated 50 times per second.

```json
{
  "device_id": "device_001",
  "timestamp": 1705593102.123,  // High-precision epoch
  "pump_status": 1,             // 0=OFF, 1=ON
  "valve_position": 45.5,       // % Open
  "tank_level": 68.2,           // % Full
  "pressure_psi": 118.4,        // Primary Physics Variable
  "flow_gpm": 42.1,             // Secondary Physics Variable
  "leak_flag": 0                // Ground Truth (0=OK, 1=LEAK)
}
```

### Aggregated Edge Message (JSON)
Generated 1 time per second (Normal Mode). Represents a summarized batch of 50 raw messages.

```json
{
  "site_id": "plant-a",
  "timestamp": 1705593103.000,
  "pressure_psi": 118.2,     // MEAN (Average)
  "flow_gpm": 41.9,          // MEAN (Average)
  "tank_level_pct": 68.3,    // MEAN (Average)
  "valve_position": 45.5,    // LAST (Snapshot)
  "pump_status": 1,          // LAST (Snapshot)
  "leak_flag": 1,            // MAX (Critical Safety Logic)
  "mode": "NORMAL"           // Metadata
}
```

**Critical Implementation Detail**:
Note that `leak_flag` uses **MAX**, not AVERAGE. If a leak exists for just 1 millisecond in the raw batch, the Aggregated Message will report `leak_flag: 1`. This ensures safety signals are never averaged away.

---

## 3. Control Command Schema

### Mode Switch Command
Sent by the Cloud Controller to reconfigure the Edge Agent.

```json
{
  "mode": "DEBUG"
}
```

### Valid Modes & Behavior
*   **`"NORMAL"`**:
    *   Aggregation Window: 1.0 seconds.
    *   Message Rate: 1 Hz.
    *   Bandwidth: ~400 bytes/sec.
*   **`"DEBUG"`**:
    *   Aggregation Window: None (Passthrough).
    *   Message Rate: 50 Hz.
    *   Bandwidth: ~20 KB/sec.
*   **`"ECONOMY"`**:
    *   Aggregation Window: 300 seconds (5 mins).
    *   Message Rate: 0.003 Hz.
    *   Bandwidth: ~1 byte/sec (avg).

---

## 4. Bandwidth Calculation (The Math)

**Baseline (50Hz)**:
*   Payload: ~400 bytes (JSON overhead + data)
*   Rate: 50 msg/sec
*   Total: 20 KB/s per sensor.
*   **10 Sensors = 200 KB/s constant.**

**Adaptive Edge (Normal)**:
*   Payload: ~400 bytes
*   Rate: 1 msg/sec
*   Total: 0.4 KB/s per sensor.
*   **Savings: (20 - 0.4) / 20 = 98% Reduction.**

This clearly provides the mathematical proof for RQ1 (Efficiency).
