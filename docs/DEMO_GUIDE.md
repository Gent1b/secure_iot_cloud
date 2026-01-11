# Thesis Demo Guide

This guide explains how to demonstrate the "Secure IoT System with Edge Computing" using monitoring tools.

## 1. Monitoring the System

After deploying the **Dynamic Scenario**, you can monitor the system through:

- **Grafana Dashboards**: `http://<MONITORING_PUBLIC_IP>:3000`
- **InfluxDB**: Query the `water_pipeline` measurement
- **MQTT Topics**: Subscribe to `iot/control/#` and `iot/data/#`

## 2. System States

The system operates in three modes for each plant:
*   **NORMAL:** Standard operation (1-minute aggregation).
*   **DEBUG:** High-frequency mode (Raw data), triggered by Leaks or Low Cloud Load.
*   **ECONOMY:** Bandwidth-saving mode (5-minute aggregation), triggered by High Cloud Load.

## 3. Step-by-Step Demo Script

1.  **Baseline:** Monitor Grafana dashboards. All sites should be in **NORMAL** mode (or DEBUG if cloud is idle).
2.  **Trigger Leak:** Use the publisher or edge agent to inject a leak event.
3.  **Observe Reaction:**
    *   Watch Grafana metrics for Plant A to show leak detection.
    *   Subscribe to MQTT topic `iot/control/plant-a` to see the JSON command: `{"mode": "DEBUG"}`.
    *   Explain: "The Controller detected the leak and the system adapted instantly to capture high-fidelity data."
4.  **Observe Recovery:** After the leak is resolved and ~1 minute passes (Controller loop), the system should return to **NORMAL**.

## 4. Troubleshooting
*   **No Data in Grafana?** Check if the subscriber and InfluxDB containers are running: `docker ps` on Cloud Node.
*   **MQTT Issues?** Verify broker connectivity: `mosquitto_sub -h <BROKER_IP> -t "iot/data/#" -c 1`

