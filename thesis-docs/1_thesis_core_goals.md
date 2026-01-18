# Thesis Core Goals & Research Hypothesis

## 1. Introduction: The Critical Infrastructure Dilemma
Modern Critical Infrastructure (CI) systems—such as water distribution, oil pipelines, and power grids—are undergoing a digital transformation known as Industry 4.0. Operators are moving from legacy SCADA systems to Cloud-native IoT architectures to leverage Machine Learning and advanced analytics.

However, this transition introduces a fundamental "Efficiency-Safety Paradox":
*   **The Safety Requirement**: Physical anomalies (pipe bursts, leakages, pressure shocks) occur on millisecond timescales. Detecting them requires **High-Fidelity, High-Frequency Data** (e.g., >50Hz sampling).
*   **The Efficiency Requirement**: Cloud bandwidth and storage are expensive. Transmitting 50Hz data from thousands of sensors 24/7 generates massive amounts of redundant data during normal operation (99% of the time).

**The Core Problem**: Traditional architectures force a compromise.
1.  **Centralized Cloud**: Safe but inefficient. (High Bandwidth cost).
2.  **Static Edge**: Efficient but potentially unsafe. (Low resolution misses transient events).

## 2. Thesis Statement & Hypothesis
**Thesis Statement**:
"A Hierarchical, Resource-Aware Edge-Cloud Architecture can resolve the Efficiency-Safety paradox by dynamically adapting data fidelity based on real-time context."

**Formal Hypothesis**:
We hypothesize that by implementing a **Closed Feedback Loop** between the Cloud (Brain) and the Edge (Reflexes), we can achieve:
1.  **Bandwidth Efficiency**: Comparable to static edge systems (~98% reduction vs baseline).
2.  **Operational Safety**: Comparable to centralized systems (<1s leak detection latency).
3.  **Resource Resilience**: The ability to prioritize safety-critical data over routine telemetry during periods of cloud congestion.

## 3. Research Questions (RQs)
Your thesis answers these three specific questions using empirical data:

*   **RQ1 (Efficiency)**: Can an adaptive edge system achieve significant bandwidth reduction (approx. 90%+) compared to a centralized baseline during normal operation?
*   **RQ2 (Responsiveness)**: Does the introduction of a cloud feedback loop introduce unacceptable latency for critical event detection compared to local processing?
*   **RQ3 (Stability)**: Can a "Max-Min Fairness" algorithm effectively prevent system oscillation (flip-flopping) while managing competing priorities (Leak Detection vs. Cloud CPU Load)?

## 4. The "Point" of the Project
This project serves as an **architectural proof-of-concept**. You are not simply building a "Water leak detector." You are proving a distributed systems theory.

### What You Are Defending
In your defense, you are demonstrating that:
1.  **Intelligence must be tiered**: Low-level physics checks belong at the Edge; High-level resource allocation belongs in the Cloud.
2.  **Static configurations are obsolete**: A system that cannot change its behavior (sampling rate) in response to crisis is either wasteful or dangerous.
3.  **Feedback Loops are viable**: We can successfully close the loop (Sense → Cloud → Edge) fast enough to be useful for industrial control.

---

## 5. Key Definitions & Concepts

### The "Dual-Trigger" Mechanism
The uniqueness of this thesis lies in its **Dual-Trigger** adaptability:
1.  **Bottom-Up Trigger (Safety)**: The *Edge* detects a leak and demands high bandwidth. This is a "Safety" override.
2.  **Top-Down Trigger (Capacity)**: The *Cloud* monitors its own CPU load. If overloaded, it suppresses bandwidth. If idle, it requests more data.

### Fairness & Priority
The system implements a specific prioritization logic ("Max-Min Fairness") to resolve conflicts:
*   *Conflict*: Cloud is 90% busy (Congested), BUT a leak occurs at Site A.
*   *Solution*: The algorithm prioritizes the Leak. Site A gets bandwidth; Site B and C are sacrificed (throttled). Safety > Efficiency.

### Ground Truth
To scientifically validate the system, we track "Ground Truth" (the actual simulated leak state) separately from "Detected State". This allows us to measure:
*   **True Positives**: System correctly alarms.
*   **False Negatives**: System misses a leak (Safety failure).
*   **Detection Latency**: Time difference between `Leak Start` and `Mode Switch`.
