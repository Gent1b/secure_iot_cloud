# Thesis Experiment User Guide

This guide explains how to run the three experimental scenarios for the Master's Thesis.

## Prerequisites
1.  **SSH Access**: Ensure you have SSH access to all 4 VMs (`mqtt`, `cloud`, `devices`, `monitoring`) configured in `~/.ssh/config`.
2.  **PowerShell**: Run commands from the root of this repository on your laptop.

## 1. Update Codebase
Before running any scenario, sync the latest code to all VMs:
```powershell
.\update.ps1
```

## 2. Scenario A: Baseline (Cloud-Centric)
*Description*: All devices send raw data directly to the Cloud. No Edge processing.

**Run:**
```powershell
.\deploy.ps1 -Scenario baseline
```
**Verify:**
*   Check InfluxDB on Cloud Node.
*   Observe high network traffic on Cloud Node.

## 3. Scenario B: Static Edge (Bandwidth Reduction)
*Description*: Edge Nodes aggregate data (1-minute averages) before sending.

**Run:**
```powershell
.\deploy.ps1 -Scenario static-edge
```
**Manual Steps (First Time Only):**
1.  SSH into `devices` VM.
2.  Run: `cd secure_iot_cloud/deployments/02_edge_static && chmod +x deploy.sh && ./deploy.sh`

**Verify:**
*   Check `iot/data/processed` topic on Central Broker.
*   Observe reduced network traffic.

## 4. Scenario C: Dynamic Edge (Feedback Loop)
*Description*: Cloud Controller dynamically adjusts Edge sampling rate based on leaks or CPU load.

**Run:**
```powershell
.\deploy.ps1 -Scenario dynamic-edge
```
**Manual Steps:**
1.  **Edge**: Same as Scenario B (ensure K3s is running).
2.  **Cloud**: SSH into `cloud` VM and run:
    ```bash
    cd secure_iot_cloud/cloud-node
    docker-compose up -d --build
    ```
    (This starts the `controller` service).

**Verify:**
*   **Trigger Leak**: Wait for a simulated leak.
*   **Observe**: The specific site switches to High-Frequency mode (DEBUG) in logs.
*   **Trigger Congestion**: On Cloud VM, run `touch /tmp/cloud_stress_test`.
*   **Observe**: All sites switch to Low-Frequency mode (ECONOMY).
