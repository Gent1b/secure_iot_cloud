# Secure IoT System Architecture: Adaptive Edge Feedback Loop
## Master's Thesis - System Documentation

### 1. High-Level Architecture
The system follows a hierarchical **Edge-Fog-Cloud** topology, designed to minimize bandwidth usage while maintaining high fidelity during critical events.

#### **Physical Layer (Virtual Machines)**
| Node Name | IP Address | Role | Software Stack |
| :--- | :--- | :--- | :--- |
| **`devices`** | `172.31.X.X` | **Edge Computing Node** | **K3s (Kubernetes)**, Python Agents, Local Mosquitto |
| **`mqtt`** | `54.93.230.47` | **Central Gateway** | Mosquitto (Bridge Mode) |
| **`cloud`** | `172.31.X.X` | **Core Intelligence** | InfluxDB, Controller Service |
| **`monitoring`** | `172.31.X.X` | **Observability** | Prometheus, Grafana, Node Exporter |

---

### 2. Logical Data Flow (The Feedback Loop)

The system operates in a closed loop: **Sense -> Decide -> Act**.

#### **A. Upstream Data Path (Telemetry)**
1.  **Sensor Generation (`water-sensors` Pod)**:
    *   Simulates `Pressure`, `Flow`, `Valve Position` (Physics Model).
    *   Publishes to **Local Broker** (`iot-edge/local-broker`) on topic `iot/devices`.
    *   *Frequency:* 50Hz (Raw).

2.  **Edge Processing (`edge-agent` Pod)**:
    *   Subscribes to **Local Broker**.
    *   **Logic (The Filter):**
        *   **NORMAL Mode:** Aggregates 60s of data -> 1 Avg Record.
        *   **ECONOMY Mode:** Aggregates 5m of data -> 1 Avg Record.
        *   **DEBUG Mode:** Passthrough (No Aggregation) -> Sends every 20ms.
        *   **LEAK DETECTED:** Immediate High-Priority Alert (Bypasses Aggregation).
    *   Publishes to **Central Broker** (`mqtt` VM) on topic `iot/data/{site_id}`.

3.  **Cloud Ingestion (`subscriber` Service)**:
    *   Subscribes to **Central Broker**.
    *   Validates data and writes to **InfluxDB** (`bucket: iot_data`).

#### **B. Downstream Control Path (Feedback)**
1.  **State Analysis (`controller` Service)**:
    *   **Polls InfluxDB:** Checks for `leak_flag=1` events (Last 1m).
    *   **Polls Prometheus:** Checks `node_cpu_usage` of the Cloud VM.

2.  **Decision Algorithm (Fairness Logic)**:
    *   **IF Leak Detected:**
        *   Target Site -> `DEBUG` (Max Visibility).
        *   Other Sites -> `ECONOMY` (Save Bandwidth).
    *   **IF Cloud CPU > 80%:**
        *   All Sites -> `ECONOMY` (Shed Load).
    *   **IF Cloud CPU < 10% (Idle):**
        *   All Sites -> `DEBUG` (Utilize Capacity).
    *   **ELSE:**
        *   All Sites -> `NORMAL`.

3.  **Actuation (Command Propagation)**:
    *   Controller publishes command to **Central Broker** on `iot/control/{site_id}`.
    *   Payload: `{"mode": "DEBUG", "timestamp": ...}`.
    *   **Edge Agent** receives command and updates its internal state `CURRENT_MODE`.

---

### 3. Component Details & Communication

#### **Edge Layer (K3s Namespace: `iot-edge`)**
*   **`local-broker` (Service)**:
    *   Internal Cluster IP.
    *   Decouples sensors from the internet.
*   **`water-sensors` (Deployment)**:
    *   *Current Implementation:* Multi-threaded Python script.
    *   *Proposed Upgrade:* AsyncIO High-Performance Generator.
*   **`edge-agent` (Deployment)**:
    *   **State Machine:** Maintains `data_buffer` and `CURRENT_MODE`.
    *   **Metric Exporter:** Exposes Prometheus metrics on port 8000 (`edge_ingress_msgs`, `edge_mode`).

#### **Cloud Layer (Docker Compose)**
*   **`controller`**:
    *   Python loop (10s interval).
    *   Queries InfluxDB API & Prometheus API.
*   **`subscriber`**:
    *   Worker thread architecture.
    *   Calculates `latency` (Time Received - Time Generated).

---

### 4. Critical Metrics for Thesis

| Metric | Source | Purpose |
| :--- | :--- | :--- |
| **Bandwidth Reduction (%)** | `edge_egress_messages` vs `edge_ingress_messages` | Proves efficiency of Edge Computing. |
| **Reaction Latency (ms)** | `iot_reaction_latency_seconds` (Histogram) | Proves "Leak Alerts" are fast even in Economy mode. |
| **Cloud CPU Load** | `node_cpu_seconds_total` (Prometheus) | Triggers the Feedback Loop. |
| **System Stability** | `up` (Prometheus) | Proves the system doesn't crash under load. |

---

### 5. Current Implementation Status

| Component | Status | Notes |
| :--- | :--- | :--- |
| **Physics Simulation** | ✅ Ready | Good pump/valve logic. |
| **Edge Agent Logic** | ✅ Ready | Aggregation & Leak detection working. |
| **Cloud Controller** | ✅ Ready | Feedback loop logic implemented. |
| **Infrastructure** | ✅ Ready | K3s & Docker VM setup complete. |
| **Performance** | ⚠️ **Risk** | Publisher uses Threads (Low Scale Limit). |
| **Orchestration** | ✅ Ready | PowerShell scripts handle deployment. |

