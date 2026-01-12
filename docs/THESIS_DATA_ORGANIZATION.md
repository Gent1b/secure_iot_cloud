# Thesis Data Dictionary

## 1. What Data Do you Actually Have?
You have three types of data. You only need to care about the **Bold** ones for your thesis.

### A. Sensor Data (The "Input")
*Source: Edge Devices (Water Pumps)*
| Field | Meaning | Why it matters |
|-------|---------|----------------|
| **`pressure_psi`** | Water pressure | Drops when there is a leak. |
| **`flow_gpm`** | Water flow rate | Spikes when there is a leak (water rushing out). |
| `tank_level` | Water in tank | Context only (ignore for main thesis). |
| **`leak_flag`** | True/False | **The Trigger.** If True -> System must react. |

### B. System State (The "Decision")
*Source: Edge Agents & Cloud Controller*
| Field | Meaning | Why it matters |
|-------|---------|----------------|
| **`operating_mode`** | Normal/Debug/Economy | **The Result.** Shows your system adapting. |
| `msg_count` | Messages sent | Proof of efficiency (bandwidth saving). |

### C. Resource Metrics (The "Constraint")
*Source: Infrastructure (Prometheus)*
| Field | Meaning | Why it matters |
|-------|---------|----------------|
| **`cpu_usage`** | Cloud Server Load | **The Limit.** If high -> System must throttle (Economy). |
| `latency` | Network delay | Secondary proof (ignore if overwhelmed). |

---

## 2. Organization Strategy: Structure Your "Results" Chapter

Don't dump all data. Organize it into **3 Test Cases**.

### Test Case 1: "Baseline Operation" (The Boring Part)
*   **Goal:** Prove the system is efficient when nothing bad is happening.
*   **Data to show:**
    *   `operating_mode` = "NORMAL" (flat line).
    *   `msg_count` = Low (approx 1 per minute).
    *   `cpu_usage` = Low (< 5%).
*   **Conclusion:** "System consumes minimal resources."

### Test Case 2: "Leak Detection Scenario" (The Action Part)
*   **Goal:** Prove the system reacts to danger.
*   **Data to show:**
    *   `leak_flag`: Changes 0 -> 1.
    *   `operating_mode`: Automatically switches "NORMAL" -> "DEBUG".
    *   `msg_count`: Spikes (approx 50 per second).
*   **Conclusion:** "System sacrifices efficiency for safety when required."

### Test Case 3: "Cloud Overload Scenario" (The Protection Part)
*   **Goal:** Prove the system prevents crashes.
*   **Data to show:**
    *   `cpu_usage`: Increases > 80% (simulated).
    *   `operating_mode`: Automatically switches "DEBUG" -> "ECONOMY".
    *   `msg_count`: Drops to near zero.
*   **Conclusion:** "System protects the cloud infrastructure under load."

---

## 3. How to Extract This Data (Simple Tables)

You don't need raw logs. You need **Summary Tables**.
I can write a script for you that just prints these exact tables.

**Example Output You Can Copy-Paste into Word/LaTeX:**

| Scenario | Mode | Avg Bandwidth (kbps) | Leak Detection Time |
|----------|------|----------------------|---------------------|
| Baseline | NORMAL | 1.2 | N/A |
| Leak Event | DEBUG | 45.6 | 0.8s |
| High Load | ECONOMY | 0.2 | N/A |

**Do you want me to create a Python script (`scripts/generate_thesis_tables.py`) that queries your InfluxDB and outputs these simple summary tables for you?**
