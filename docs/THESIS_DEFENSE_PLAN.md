# Thesis Defense Strategy: Secure IoT with Adaptive Feedback

## 1. The Core Narrative (The "Story")
Your thesis isn't just about "connecting sensors." It solves a specific conflict in IoT:
> **"How do we get high-quality data for safety (Leak Detection) without crashing the cloud or incurring huge costs?"**

### The Conflict
- **Safety** requires high-frequency data (50Hz) to spot sudden bursts/leaks.
- **Efficiency** requires low-frequency data (1 min averages) to save bandwidth and storage.
- **Static systems fail:** They either miss leaks (too slow) or crash the server (too much data).

### Your Solution
**The Adaptive Feedback Loop.**
Your system is "smart" because it changes its behavior based on the situation. It negotiates between the **Edge** (Safety) and the **Cloud** (Capacity).

---

## 2. Mapping Data to Defense Arguments

You are collecting a lot of data, but you only need to show 3 specific relationships to defend your thesis.

### Argument A: "The System Prioritizes Safety"
*   **The Question:** "What happens if a pipe bursts? Do we miss it?"
*   **The Evidence:**
    *   **Data:** `leak_flag` (from sensors) vs. `operating_mode` (from edge agent).
    *   **Visualization:** A time-series graph.
    *   **What to point at:** "Look at **Time X**. The moment the leak started, the system forced the mode to `DEBUG` (High Res) to capture details, ignoring bandwidth costs."

### Argument B: "The System is Resource Efficient"
*   **The Question:** " Why not just run in High Resolution (Debug) mode all the time?"
*   **The Evidence:**
    *   **Data:** `data_rate` (messages per second) in `NORMAL` vs `DEBUG` mode.
    *   **What to point at:** "Running in `DEBUG` everywhere would mean 3000 msg/sec. My `NORMAL` mode reduces this by **98%**, saving massive cloud costs while everything is fine."

### Argument C: "The System Protects the Cloud (Stability)"
*   **The Question:** "What if the cloud server gets overloaded?"
*   **The Evidence:**
    *   **Data:** `cloud_cpu_usage` (Prometheus) vs. `operating_mode`.
    *   **What to point at:** "At **Time Y**, I simulated High CPU load. The Controller detected this and authorized `ECONOMY` mode, relieving pressure on the ingestion service."

---

## 3. How to Demonstrate It (The "Demo_Director")

You don't need to show raw tables. You show the **Cause and Effect** sequence using the script we just ran.

**The "Script" for your Demo:**

1.  **Baseline (00:00 - 01:00):**
    *   System runs in `NORMAL` (1Hz).
    *   *Defense:* "Green status. Low bandwidth. Efficient."
2.  **The Event (01:00):**
    *   You run: `python demo_director.py --action leak ...`
    *   *Defense:* "I inject a leak failure."
3.  **The Reaction (01:05):**
    *   On Dashboard: `leak_flag` goes RED. Mode switches to `DEBUG`.
    *   *Defense:* "The Edge Agent detected the anomaly locally and requested high bandwidth."
4.  **The Resolution (02:00):**
    *   You run: `python demo_director.py --action normal ...`
    *   On Dashboard: System settles back to `NORMAL`.
    *   *Defense:* "Crisis over. System returns to efficiency mode automatically."

## 4. Why This is "Master's Level" Work
You aren't just plotting data. You implemented a **Cyber-Physical System (CPS)** with:
1.  **Distributed Computing:** Logic exists on both Edge (K3s) and Cloud.
2.  **Feedback Control:** The output (Cloud CPU/Leak status) controls the input (Edge sampling rate).
3.  **Infrastructure as Code:** You can deploy the whole "world" with one script.

**Don't worry about "knowing all the data."**
Focus strictly on the **Feedback Loop**:
*   Input: `Pressure` -> `Leak`
*   Constraint: `Cloud CPU`
*   Output: `Mode` (Normal/Debug/Economy)

Everything else (container logs, TCP packets, minor metrics) is just supporting noise. Focus on those three things.
