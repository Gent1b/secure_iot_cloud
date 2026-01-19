# Secure IoT System with Edge Computing & Feedback Control
## Master's Thesis Implementation

This repository contains the complete implementation of a hierarchical IoT system designed for Critical Infrastructure monitoring (Water Utility). It demonstrates the evolution from a centralized cloud architecture to an adaptive edge-cloud system with feedback control.

---

## 1. Architecture Overview

The system is deployed across 4 Virtual Machines (VMs), representing distinct physical layers:

| VM Name | Role | Software Stack |
| :--- | :--- | :--- |
| **`devices`** | **Edge Layer**: Simulates sensors and performs local processing. | Docker (Baseline) / K3s (Edge Scenarios) |
| **`mqtt`** | **Gateway Layer**: Central message broker. | Mosquitto MQTT |
| **`cloud`** | **Cloud Layer**: Storage, Analytics, and Control. | InfluxDB, Python Subscriber, Controller |
| **`monitoring`** | **Observability**: System health monitoring. | Prometheus, Grafana |

---

## 2. Experimental Scenarios

The project implements three distinct scenarios to validate the thesis hypothesis.

### Scenario A: Baseline (Cloud-Centric)
*   **Logic**: "Dumb" devices send raw high-frequency data (50Hz) directly to the Cloud.
*   **Data Flow**: `Sensor -> Internet -> Cloud DB`
*   **Expected Behavior**:
    *   High Network Bandwidth usage (~50 messages/sec per device).
    *   High Cloud CPU usage for ingestion.
    *   No local intelligence (if network fails, data is lost).

### Scenario B: Static Edge (Bandwidth Reduction)
*   **Logic**: Edge Nodes (K3s) intercept raw data, compute 1-second aggregates (1 Hz output), and send only the aggregate to the Cloud.
*   **Data Flow**: `Sensor -> Local K3s -> Aggregator -> Internet -> Cloud DB`
*   **Expected Behavior**:
*   **~98% Bandwidth Reduction** (1 Hz vs 50 Hz).
    *   Cloud CPU usage drops significantly.
    *   "Leak Alerts" are still sent immediately (priority bypass).

### Scenario C: Dynamic Edge (Feedback Loop)
*   **Logic**: A **Cloud Controller** monitors the system and dynamically commands Edge Nodes to change their reporting mode.
*   **Feedback Algorithm**:
    1.  **Leak Detected?** -> Command specific site to `DEBUG` mode (50Hz raw data).
    2.  **High Cloud Memory?** -> Command non-leak sites to `ECONOMY` mode (hard constraint).
    3.  **High Cloud CPU?** -> Command non-leak sites to `ECONOMY` mode (soft constraint).
    4.  Otherwise -> Keep sites in `NORMAL` mode (1 Hz aggregation).
*   **Expected Behavior**:
*   System runs in `NORMAL` by default.
    *   If you simulate a leak, that specific site goes high-res while others may throttle.
    *   Demonstrates "Fairness" and "Resource Awareness".

---

## 3. How to Run

### Prerequisites
*   SSH access to all 4 VMs configured in `~/.ssh/config`.
*   PowerShell (on local machine).

### Step 1: Sync Code
Push your local changes to GitHub, then update all VMs:
```powershell
.\scripts\1_pull_updates.ps1
```

### Step 2: Reset Environment (Optional)
If you want to start fresh (wipes all data):
```powershell
.\scripts\2_reset_all.ps1
```

### Step 3: Deploy a Scenario
Choose **ONE** scenario to run at a time.

**Run Baseline:**
```powershell
.\scripts\4_deploy_scenario.ps1 -Scenario baseline
```

**Run Static Edge:**
```powershell
.\scripts\4_deploy_scenario.ps1 -Scenario static
```

**Run Dynamic Edge:**
```powershell
.\scripts\4_deploy_scenario.ps1 -Scenario dynamic
```

---

## 4. Verification & Monitoring

### Grafana Dashboards
Access Grafana at `http://<monitoring-vm-ip>:3000`.
*   **Dashboard: "IoT System Overview"**:
    *   **Throughput Panel**: In Baseline, this will be high. In Static Edge, this will be near zero.
    *   **Latency Panel**: Shows the time difference between Event Generation and Cloud Ingestion.

### InfluxDB Data Explorer
Access InfluxDB at `http://<cloud-vm-ip>:8086`.
*   **Bucket**: `iot_data`
*   **Measurement**: `water_pipeline`
*   **Check**: Look for the `mode_code` field (0=NORMAL, 1=DEBUG, 2=ECONOMY) and tags like `run_id`, `scenario`, `site_id`, `sample_kind`.

### Troubleshooting
If something isn't working:
1.  Check Docker status on the specific node: `ssh cloud "docker ps"`
2.  Check K3s pods on device node: `ssh devices "sudo kubectl get pods -n iot-edge"`
3.  View logs: `ssh cloud "docker logs cloud-node-controller-1"`

---

## 5. Project Structure

```
secure_iot_system_cloud/
├── scripts/                 # Automation scripts (Start here!)
├── deployments/             # K3s manifests and scenario-specific configs
├── cloud-node/              # Cloud services (InfluxDB, Controller)
├── device-node/             # Baseline device simulation
├── subscriber/              # Edge Agent & Cloud Subscriber logic
├── publisher/               # Water Pump Simulation logic
└── docs/                    # Architecture diagrams and detailed guides
```
