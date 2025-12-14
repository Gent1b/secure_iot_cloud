# Thesis Architecture Specification: Adaptive Edge-Cloud IoT System

## 1. System Overview
This thesis implements a **Hierarchical IoT Architecture** for Critical Infrastructure (Water Utility) monitoring. The system moves beyond traditional centralized cloud collection by introducing an **Edge Layer** capable of local data reduction and a **Feedback Loop** where the Cloud dynamically orchestrates Edge behavior based on system-wide constraints (Fairness/Bandwidth).

### Core Design Principles
1.  **Data Locality**: High-frequency raw data (50Hz) never leaves the local network unless explicitly requested.
2.  **Decoupled Control**: The "Control Plane" (Commands) is separate from the "Data Plane" (Telemetry).
3.  **Fail-Safe Operation**: The Edge Node can operate autonomously (detecting leaks locally) even if the Cloud connection is severed.

---

## 2. The 3-Layer Topology

We utilize the existing 4 VMs to represent physically distinct layers.

```mermaid
graph TD
    subgraph "Layer 1: Edge (devices-node)"
        style P fill:#f9f,stroke:#333,stroke-width:2px
        P[Water Sensors (Publishers)] -->|Raw 50Hz| LB[Local MQTT Broker]
        LB -->|Raw 50Hz| EA[Edge Agent (K3s Pod)]
        EA -->|Feedback Cmds| EA
    end

    subgraph "Layer 2: Transport (mqtt-node)"
        EA == "Filtered Data (1Hz) / Alerts" ==> CB[Central MQTT Broker]
        CB -. "Control Commands" .-> EA
    end

    subgraph "Layer 3: Cloud (cloud-node)"
        CB -->|Ingest| CS[Cloud Subscriber]
        CS --> DB[(InfluxDB)]
        DB --> CTL[Feedback Controller]
        CTL -.->|Publish Cmd| CB
    end

    subgraph "Observability (monitoring-node)"
        PROM[Prometheus] -- Scrapes --> EA
        PROM -- Scrapes --> CS
        PROM -- Scrapes --> CB
    end
```

### Layer 1: The Edge (VM: `devices-node`)
*   **Role**: Local ingestion, processing, and filtering.
*   **Infrastructure**: K3s (Kubernetes) Cluster.
*   **Components**:
    1.  **Publishers (Pods)**: Simulate Water Pumps/Valves. They publish to `localhost:1883` (Local Broker). They are *unaware* of the cloud.
    2.  **Local MQTT Broker (Service)**: A lightweight Mosquitto instance running *inside* the K3s cluster. It buffers the high-speed raw data.
    3.  **Edge Agent (Pod)**: The "Brain" of the edge.
        *   **Dual-Client Architecture**: It has one MQTT client listening to `Local Broker` and another MQTT client talking to `Central Broker`.
        *   **Logic**: Calculates rolling averages, detects leaks, and executes commands received from the Cloud.

### Layer 2: The Gateway (VM: `mqtt-node`)
*   **Role**: WAN Gateway and Message Routing.
*   **Infrastructure**: Docker (Mosquitto).
*   **Components**:
    1.  **Central MQTT Broker**: The only entry point to the cloud network. It handles authentication and routing between Edge sites and Cloud services.

### Layer 3: The Cloud (VM: `cloud-node`)
*   **Role**: Long-term storage, global analytics, and orchestration.
*   **Infrastructure**: Docker Compose.
*   **Components**:
    1.  **Cloud Subscriber**: Dumb pipe. Subscribes to `Central Broker` and writes to InfluxDB.
    2.  **Feedback Controller**: The "Manager".
        *   **Inputs**:
            *   **Application State**: "Is there a leak at Site A?" (High Priority).
            *   **Infrastructure State**: "Is Cloud CPU < 50%?" (Capacity available).
        *   **Logic**: Implements a **Resource-Aware Fairness Algorithm**.
        *   **Outputs**: Commands to Edge Nodes (e.g., `{"site": "site_a", "rate": "50Hz"}`).

---

## 3. The Feedback Loop & Fairness Algorithm

This is the scientific core of the thesis. It combines **Application Requirements** (Leaks) with **Infrastructure Constraints** (Cloud Capacity).

### The 3 Edge Nodes (Logical Sites)
Although running on one physical VM (`devices-node`), we simulate **3 Independent Edge Sites** using K3s namespaces/labels:
1.  **Site A**: Critical Pump Station.
2.  **Site B**: Distribution Hub.
3.  **Site C**: Remote Reservoir.

### The Fairness Algorithm
The Controller runs a **Max-Min Fairness** logic loop every 10 seconds:

1.  **Measure Capacity**: Check Cloud Node CPU and InfluxDB Write Latency.
    *   *Target*: Keep CPU < 80%.
2.  **Determine Demand**:
    *   *Normal*: All sites want to send 1Hz heartbeat.
    *   *Leak Event*: Site A needs 50Hz (High Demand).
    *   *Underutilized*: If Cloud CPU is low (< 20%), request **High-Fidelity Data** from all sites for model training.
3.  **Allocate Bandwidth**:
    *   *Scenario 1 (Congestion)*: Site A has leak. Site A gets 80% of capacity. Sites B & C throttled to 0.1Hz.
    *   *Scenario 2 (Abundance)*: Cloud is idle. All sites commanded to send 10Hz raw data to fill capacity (Data Enrichment).

### Feedback Scenarios

#### Scenario A: The "Leak" (Priority Override)
*   **Event**: Site A detects leak.
*   **Action**: Controller grants Site A full bandwidth. Site B/C are throttled.
*   **Result**: "Unfair" allocation justified by Critical Infrastructure safety.

#### Scenario B: The "Idle Cloud" (Resource Maximization)
*   **Event**: System is stable. Cloud CPU is 10%.
*   **Action**: Controller broadcasts `OPTIMIZE_TRAINING_DATA` command.
*   **Result**: All Edge Nodes switch to "Batch Upload" mode, sending cached high-res data to utilize the idle cloud resources for long-term analytics.

---

## 4. Implementation Roadmap

### A. Infrastructure Updates
1.  **`devices-node`**: Install K3s. Deploy a `mosquitto` deployment *inside* K3s (ClusterIP service).
2.  **`mqtt-node`**: Remains as is (Central Broker).

### B. Code Refactoring

**1. `publisher/device.py`**
*   **Change**: Hardcode connection to `localhost` (or K8s service name `edge-broker`). Remove all logic about "Cloud". It just dumps data.

**2. `subscriber/edge_agent.py` (NEW)**
*   This is a *new* script derived from your current subscriber.
*   **Inputs**:
    *   `LOCAL_BROKER_URL`: (e.g., `edge-broker`)
    *   `CENTRAL_BROKER_URL`: (e.g., `54.93.230.47`)
*   **Logic**:
    *   `on_message_local()`: Process raw data.
    *   `on_message_central()`: Listen for commands (`iot/control/+`).
    *   `publish_upstream()`: Send processed data to Central.

**3. `cloud/controller.py` (NEW)**
*   **Logic**: A control loop that runs every 10 seconds.
*   Checks InfluxDB for "Leak Probability" flags.
*   Publishes JSON commands to `iot/control/{site_id}`.

### C. Topics Schema (Strict)

| Topic | Publisher | Subscriber | Purpose |
| :--- | :--- | :--- | :--- |
| `local/sensors/{id}` | Device | Edge Agent | Raw 50Hz data (Local only) |
| `iot/data/{site_id}` | Edge Agent | Cloud Sub | Aggregated data / Alerts |
| `iot/control/{site_id}` | Controller | Edge Agent | Commands (`{"mode": "DEBUG"}`) |
