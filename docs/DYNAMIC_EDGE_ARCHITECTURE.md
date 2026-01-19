# Dynamic Edge Architecture & Feedback Loop

This document details the **Dynamic Edge Scenario (Scenario C)**, which represents the core contribution of this Master's Thesis. It implements a **Cyber-Physical Feedback Loop** that balances application requirements (Leak Detection) with infrastructure constraints (Cloud CPU/Bandwidth).

---

## 1. Conceptual Overview

In traditional IoT (Scenario A), devices are "dumb" and send all data to the cloud. In Static Edge (Scenario B), devices are "smart" but rigid (always aggregate).

**Dynamic Edge (Scenario C)** introduces **Adaptability**. The system behaves like a living organism:
*   **Relaxed State**: When the system is healthy, devices stay in **NORMAL** (1 Hz aggregation).
*   **Stressed State**: When the cloud is overloaded, devices automatically throttle down to save resources.
*   **Emergency State**: If a leak is detected, the specific affected site ignores all throttling rules and sends high-fidelity data immediately.

---

## 2. Architecture Components

The architecture is a **3-Layer Hierarchy**:

### Layer 1: The Device (Edge Node)
*   **Hardware**: Simulated by the `devices` VM.
*   **Software**: K3s (Kubernetes) running `edge_agent.py`.
*   **Role**:
    *   Acts as a **Local Gateway**.
    *   Runs a local MQTT Broker (`mosquitto`) so sensors don't need Internet.
    *   Performs **Edge Analytics** (Leak Detection).
    *   Buffers and Aggregates data based on the current **Mode**.

### Layer 2: The Network (MQTT Bus)
*   **Hardware**: `mqtt` VM.
*   **Software**: Mosquitto.
*   **Role**: The central nervous system. It carries:
    *   **Uplink**: Sensor Data (`iot/data/+`).
    *   **Downlink**: Control Commands (`iot/control/+`).

### Layer 3: The Cloud (Brain)
*   **Hardware**: `cloud` VM.
*   **Software**: Docker Compose (`controller.py`, `influxdb`, `subscriber`).
*   **Role**:
    *   **Ingestion**: Stores data in InfluxDB.
    *   **Controller**: The "Brain" that makes decisions.

---

## 3. Data Flow

### Path A: Normal Operation (Aggregation)
1.  **Sensor** generates data (50Hz) -> Publishes to **Local Broker** (on Device).
2.  **Edge Agent** receives data locally.
3.  **Edge Agent** checks: *Is this a leak?* (No).
4.  **Edge Agent** checks: *What is my Mode?* (NORMAL).
5.  **Action**: Data is **Aggregated** (streaming) in memory.
6.  **Timer**: Every 1 second, a single aggregate point is emitted (1 Hz output).
7.  **Uplink**: One single JSON summary is sent to **Central Broker**.
8.  **Cloud**: Subscriber saves summary to InfluxDB.

### Path B: Emergency (Leak Detected)
1.  **Sensor** generates data (High Flow, Low Pressure).
2.  **Edge Agent** receives data locally.
3.  **Edge Agent** checks: *Is this a leak?* (**YES**).
4.  **Action**: **Bypass Buffer**.
5.  **Uplink**: The raw alert packet is sent **Immediately** to **Central Broker**.
6.  **Cloud**: Controller sees the alert flag.

### Path C: The Feedback Loop (Control)
1.  **Controller** (Cloud) wakes up every 10 seconds.
2.  **Input 1**: Queries InfluxDB. *Did we receive `leak_detected` in the last window?*
3.  **Input 2**: Queries Prometheus. *Are cloud memory/CPU above thresholds?*
4.  **Decision**:
    *   *Case 1 (Leak)*: Send `{"mode": "DEBUG"}` to the specific site (never overridden).
    *   *Case 2 (High Memory)*: Send `{"mode": "ECONOMY"}` to non-leak sites.
    *   *Case 3 (High CPU)*: Send `{"mode": "ECONOMY"}` to non-leak sites.
    *   *Else*: Send `{"mode": "NORMAL"}`.
5.  **Downlink**: Publishes command to `iot/control/site-a`.
6.  **Edge Agent** receives command -> Updates global variable `CURRENT_MODE`.
7.  **Result**: Edge Agent changes its aggregation behavior instantly.

---

## 4. Code Mapping

| Component | File Path | Key Functionality |
| :--- | :--- | :--- |
| **Edge Agent** | `subscriber/edge_agent.py` | `detect_leak()`: Physics logic.<br>`on_local_message()`: Buffer vs Send logic.<br>`on_central_message()`: Mode switching. |
| **Controller** | `cloud-node/controller.py` | `check_system_state()`: The main loop.<br>`influx_client.query()`: Checks for leaks.<br>`mqtt_client.publish()`: Sends commands. |
| **Deployment** | `deployments/03_edge_dynamic/` | `deploy.sh`: Scripts to install this specific logic on K3s. |

## 5. Operating Modes

| Mode | Description | Aggregation | Use Case |
| :--- | :--- | :--- | :--- |
| **DEBUG** | Passthrough | None (0s) | Leak Analysis, System Debugging |
| **NORMAL** | Standard | 1s (1 Hz output) | Day-to-day monitoring |
| **ECONOMY** | Low Power | High (5m) | Cloud Congestion, Cost Saving |
