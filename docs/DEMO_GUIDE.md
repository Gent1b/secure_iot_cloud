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

1.  **Baseline:** Show the dashboard. All sites should be in **NORMAL** mode (or DEBUG if cloud is idle).
2.  **Trigger Leak:** Click **🔥 LEAK (Plant A)**.
3.  **Observe Reaction:**
    *   Watch the "System State" for Plant A flip to **DEBUG**.
    *   Check the "Live Log" for the JSON command: `{"mode": "DEBUG"}`.
    *   Explain: "The Edge Agent detected the leak (or Cloud did) and the system adapted instantly to capture high-fidelity data."
4.  **Resolve:** Click **✅ Normal (Plant A)**.
5.  **Observe Recovery:** After ~1 minute (Controller loop), the system should return to **NORMAL**.

## 4. Troubleshooting
*   **No Data?** Click "Reconnect MQTT" in the sidebar.
*   **Connection Refused?** Check if the Frontend container is running: `docker ps` on Cloud Node.
