# Thesis Data Sufficiency & Comparability Verification

**Date:** January 12, 2026  
**Subject:** Validation of Experimental Data for Thesis Scenarios

This document validates whether the collected data from the `secure_iot_system_cloud` implementation is sufficient to support the comparative analysis between the three thesis scenarios:
1.  **Baseline** (Centralized Cloud)
2.  **Static Edge** (Fixed Aggregation)
3.  **Adaptive Feedback** (Dynamic Edge-Cloud Loop)

---

## 1. Core Metrics Checklist & Comparability

The following metrics are present and comparable across all three scenarios:

| Metric Category | Metric Name | Baseline | Static Edge | Adaptive Feedback | Status |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Bandwidth** | `iot_bandwidth_bytes_total` | Raw Stream (High) | Aggregated (Low) | Variable (Dynamic) | **READY** |
| **Latency*** | `iot_end_to_end_latency_seconds` | Network Delay Only | Net + Processing | Net + Proc | **READY*** |
| **Cloud Load** | `influxdb_writes_success_total` | High IOPS | Low IOPS | Variable IOPS | **READY** |
| **Leak Visibility** | `leak_flag` | Individual Points | Max of Window + Alerts | Real-time on Trigger | **READY** |
| **System State** | `edge_operating_mode` | N/A | Constant | Variable (0, 1, 2) | **READY** |

***Latency Methodology Warning:**  
In aggregated scenarios ("Static" and "Adaptive: Normal"), the timestamp of a record corresponds to the *end* of the aggregation window. A "low" network latency metric (ms) does not reflect "Data Freshness" (which may be up to 60s old).  
**Action:** Define "Information Latency" vs. "Network Latency" separately in the methodology chapter.

---

## 2. Adaptive Scenario Specifics

To prove the "Adaptive" claim, specific signals must exist to show the loop in action.

*   **Controller Inputs (Sense):**
    *   The controller accesses `leak_flag` (Application State) and `cloud_cpu` (Infrastructure State).
    *   *Validation:* Confirmed via `controller.py` logs (stdout).
*   **Controller Output (Decide & Act):**
    *   *Weakness:* No direct counter for "Decisions Made".
    *   *Mitigation:* Use the `edge_operating_mode` metric in PromQL. A change in this integer value (e.g., $1 \rightarrow 2$) is definitive proof a decision was received and applied.

---

## 3. Cause-Effect Analysis Capability

The dataset supports the required causal chains:

### Chain A: Leak Event Response
1.  **Event:** `leaks_detected` (Counter) increments.
2.  **Decision:** Controller logs "CRITICAL LEAK DETECTED".
3.  **Result:** `edge_operating_mode` switches to `2` (DEBUG).
4.  **Proof:** `iot_bandwidth_bytes_total` rate spikes ~10-20 seconds later.

### Chain B: Resource Maximization (Idle Cloud)
1.  **Event:** `node_cpu` (Prometheus) drops below 20%.
2.  **Decision:** Controller logs "CLOUD IDLE".
3.  **Result:** `edge_operating_mode` switches to `2` (DEBUG).
4.  **Proof:** Bandwidth utilization increases to fill available capacity.

---

## 4. Sampling & Normalization

*   **Aggregation Windows:** Consistent across Static and Adaptive (Normal Mode = 60s, Economy = 300s).
*   **Comparability:** Timelines are physical and consistent.
*   **Recommendation:** When plotting comparisons, normalize the x-axis to "Experiment Time (minutes)" and y-axis to "KB/min" to handle the different reporting frequencies visually.

---

## 5. Final Assessment

**Is the dataset thesis-defensible?**  
**YES.** The instrumentation is now sufficient to support the claims of Bandwidth Optimization, Safety Preservation, and Resource Awareness.

### Checklist for Thesis Write-up
- [ ] **Define Latency Terms:** Distinctly define "Network Latency" vs "Freshness".
- [ ] **Log Evidence:** Explicitly state that Controller Logs are used to validate decision logic timestamps.
- [ ] **Normalization:** Use "Bytes/Minute" for bandwidth comparisons.
