# Experimental Scenarios

To explicitly prove the thesis hypothesis, we use a "Comparative Analysis" methodology. We deploy the same software stack in three different configurations (Scenarios) to isolate the variables of *Efficiency* and *Adaptability*.

---

## Scenario 1: The Baseline (Centralized)
**"The Naive Cloud Approach"**

### Configuration
*   **Deployment**: `deployments/01_baseline_docker/`
*   **Edge Processing**: **DISABLED**. Sensors publish directly to the Cloud Broker.
*   **Sampling Rate**: Fixed **50Hz** (Every 20ms).
*   **Controller**: **DISABLED**. No feedback loop.

### Purpose (The "Control Group")
This represents the "Safe but Wasteful" legacy usage. It establishes the benchmarks for:
1.  **Max Bandwidth Usage**: This is our "100%" reference point.
2.  **Detection Latency**: How long does it take for data to travel `Sensor -> Transport -> Cloud -> InfluxDB`? This is the baseline latency.

### Expected Outcome
*   Bandwidth: Extremely High.
*   CPU Load: High.
*   Safety: Excellent (no data loss).

---

## Scenario 2: Static Edge (Isolated)
**"The Optimization Trap"**

### Configuration
*   **Deployment**: `deployments/02_edge_static/`
*   **Edge Processing**: **ENABLED** (K3s).
*   **Sampling Rate**: Fixed **1Hz** (Aggregated).
*   **Controller**: **DISABLED**.

### Purpose
This represents the modern "Edge First" approach that blindly optimizes for bandwidth. It proves:
1.  **Efficiency Gains**: How much bandwidth is saved by aggregating 50 messages into 1? (Expected: ~98%).
2.  **The Flaw**: It demonstrates that *transient events* (leaks lasting <1s) are averaged out and lost.

### Expected Outcome
*   Bandwidth: Very Low (~2%).
*   CPU Load: Low.
*   Safety: **Poor**. Leaks are detected locally but the high-fidelity evidence is never sent to the cloud.

---

## Scenario 3: Adaptive Edge (The Thesis Solution)
**"The Intelligent Hybrid"**

### Configuration
*   **Deployment**: `deployments/03_edge_dynamic/`
*   **Edge Processing**: **ENABLED**.
*   **Sampling Rate**: **Variable** (Controlled by Cloud).
*   **Controller**: **ENABLED** (Feedback Loop Active).

### Purpose
This is the system under test. It aims to combine the efficiency of Scenario 2 with the safety of Scenario 1.

### Independent Variables (What we change)
*   **Leak Injection**: We simulate leaks stochastically.
*   **Cloud Load**: We simulate CPU spikes (Congestion).

### Dependent Variables (What we measure)
*   **Bandwidth**: Should match Scenario 2 during calm periods.
*   **Reaction Time**: How fast does it switch to Scenario 1 mode when a leak happens?
*   **Fidelity**: Do we get the raw data during the leak?

### Expected Outcome (The "Golden Result")
*   Bandwidth: Low overall (peaks only during leaks).
*   Safety: High (Leak event triggers 50Hz mode).
*   Resilience: Bandwidth drops during CPU congestion (Protecting the cloud).

---

## Comparative Matrix

| Feature | Baseline | Static Edge | Adaptive Edge |
| :--- | :--- | :--- | :--- |
| **Logic Location** | Cloud | Edge | Shared (Hybrid) |
| **Bandwidth (Avg)** | 100% | ~2% | ~3% (varies) |
| **Leak Visibility** | Full Raw Data | Aggregated Only | Raw Data (On Demand) |
| **Congestion Response**| None (System Crash) | None (Ignored) | Throttling (Survival) |

**Success Criteria**:
Adaptive Edge must show **<10% Bandwidth** of Baseline AND **<2s Latency** for mode switching.
