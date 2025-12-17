# Thesis Demo Guide

This guide explains how to demonstrate the "Secure IoT System with Edge Computing" using the new Control Panel.

## 1. Accessing the Control Panel
After deploying the **Dynamic Scenario**, the frontend dashboard is available at:

```
http://<CLOUD_PUBLIC_IP>:8501
```

*(Ensure Port 8501 is open in your AWS Security Group)*

## 2. Dashboard Features

### System State
The dashboard shows the real-time operating mode of each plant:
*   **NORMAL:** Standard operation (1-minute aggregation).
*   **DEBUG:** High-frequency mode (Raw data), triggered by Leaks or Low Cloud Load.
*   **ECONOMY:** Bandwidth-saving mode (5-minute aggregation), triggered by High Cloud Load.

### Simulation Controls
Use the sidebar buttons to inject synthetic events:
*   **🔥 LEAK (Plant A/B):** Sends a high-flow, low-pressure reading.
    *   *Expected Result:* Controller detects leak -> Switches site to **DEBUG** mode.
*   **✅ Normal (Plant A/B):** Sends standard telemetry.
    *   *Expected Result:* If leak is resolved, Controller eventually switches back to **NORMAL**.

### Live Log
The bottom panel shows the raw MQTT messages on `iot/control/#` (Controller decisions) and `iot/data/#` (Edge telemetry).

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
