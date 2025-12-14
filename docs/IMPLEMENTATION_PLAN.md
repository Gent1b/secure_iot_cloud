# Multi-Scenario Thesis Architecture Plan

## Goal
Enable three distinct experimental modes (Baseline, Static Edge, Dynamic Edge) to validate "Fairness" and "Feedback Mechanisms" in a **Critical Infrastructure (Water/Pipeline Utility)** scenario.

## 1. The CI Data Model (Water Utility)
We will replace the simple Temp/Humidity model with a stateful **Pipeline Simulation**.
*   **Assets**: Each "Device" simulates a section of a pipeline with a Pump and a Valve.
*   **Stateful Logic**:
    - **Pump**: Cycles ON/OFF based on tank levels.
    - **Pressure**: Rises when Pump is ON and Valve is CLOSED (risk of burst). Drops when Valve is OPEN.
    - **Flow**: High when Valve is OPEN.
    - **Leak**: Rare random event where Flow is high but Pressure drops (the "Anomalous Pattern" the Edge must detect).
*   **Payload Schema**:
    ```json
    {
      "sensor_id": "pump_station_04",
      "site_id": "site_a",
      "timestamp": 17000000,
      "pressure_psi": 145.2,     // High frequency fluctuation
      "flow_gpm": 42.5,          // Flow rate
      "valve_position": 100,     // 0-100% open
      "pump_status": 1,          // 1=ON, 0=OFF
      "tank_level_pct": 55.4,    // Slowly changing
      "maintenance_mode": 0      // 1=Under Maintenance
    }
    ```
*   **Edge Processing Justification**:
    - *Naive (Cloud)*: Send 10Hz pressure readings (huge bandwidth).
    - *Edge*: Send only when `delta(pressure) > 5 PSI` OR `status_change` OR `leak_detected`.
    - *Feedback*: Cloud sees "Leak Probable" -> Commands Edge "Send 50Hz raw data for analysis".

## 2. Architecture Variations

### A. Baseline (Cloud-Centric)
*   **Deployment**: Docker Compose.
*   **Data**: All raw 1Hz/10Hz data sent to Cloud.
*   **Bottleneck**: Network Saturation + Cloud InfluxDB IOPS.

### B. Static Edge (Distributed Processing)
*   **deployment**: K3s on `device-node`.
*   **Edge Logic (`subscriber.py`)**:
    - **Aggregation**: Compute 1-minute averages for Pressure/Flow.
    - **Deadband**: Only report Tank Level if it changes by > 1%.
    - **Alerts**: Priority send if `Pressure > 150 PSI`.
*   **Result**: 90% Bandwidth reduction.

### C. Dynamic Edge (Feedback & Fairness)
*   **Scenario**: "Site A" has a leak. "Site B" and "Site C" are normal.
*   **Feedback Loop**:
    1.  **Cloud Controller**: Sees "Leak Alert" from Site A (but low resolution data).
    2.  **Action**: Sends `{"site": "site_a", "mode": "DEBUG_HIGH_RES"}`.
    3.  **Fairness**: To compensate for Site A's extra bandwidth, sends `{"site": "site_b", "mode": "LOW_RES"}` and `{"site": "site_c", "mode": "LOW_RES"}`.
    4.  **Result**: Site A gets full visibility to diagnose the leak, while total system bandwidth remains constant.

## Implementation Roadmap

### Phase 1: Data Simulation (`publisher/`)
- [ ] Refactor `device.py` to implement the `WaterSystemSimulation` class.
- [ ] Support generating "Leak Events" via a trigger file or API.

### Phase 2: Refactoring Subscriber (`subscriber/`)
- [ ] Add `water_processing_logic.py`:
    - Leak detection logic (Flow > 0 && Pressure < Threshold).
    - Rolling window averages for PSI/Flow.

### Phase 3: Controller & Deployments
- [ ] Implement `controller.py` with specific rules for "Water Leak Response".
- [ ] Create K3s manifests for 3-site deployment.
