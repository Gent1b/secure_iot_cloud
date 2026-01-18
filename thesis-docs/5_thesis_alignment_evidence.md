# Thesis Alignment & Verification Evidence

This document serves as your "Defense Bible". It maps every major engineering decision to an academic requirement. Use this to prove your work is rigorous.

---

## 1. Reproducibility Assurance
**Requirement**: "Scientific experiments must be repeatable. Random noise should not drive the results."
**Implementation Fix**:
*   **Deterministic Seeding**: Added `LEAK_SEED=42` to `publisher/device.py`.
*   **Mechanism**: The random number generator for leak simulation is seeded at startup.
*   **Result**: Every time you run the simulation, the leak occurs at the *exact same timestamp* (e.g., T+300s).
*   **Defense Value**: "I can compare Baseline vs Edge side-by-side because the 'Chaos' was mathematically identical in both runs."

## 2. Rate Control Integrity (The 50Hz Guarantee)
**Requirement**: "The Baseline must authentically represent a high-frequency stress test."
**Implementation Fix**:
*   **Monotonic Clock**: Replaced `time.sleep(0.02)` with a `while` loop checking `time.monotonic()`.
*   **Mechanism**: The loop compensates for processing time (drift) to ensure *exactly* 50 messages are sent every second.
*   **Result**: The 'Baseline' graph shows a flat, consistent 50Hz line, proving valid network stress.
*   **Defense Value**: "My bandwidth savings are real, not a result of a slow simulator."

## 3. Feedback Loop Stability (Hysteresis)
**Requirement**: "Control systems must not oscillate (bang-bang control) in response to noise."
**Implementation Fix**:
*   **Hysteresis Logic**: Added to `cloud-node/controller.py`.
    *   *Enter Economy*: CPU > 80%.
    *   *Exit Economy*: CPU < 60%.
*   **Cooldown**: Added a 30-second lockout timer after any mode switch.
*   **Result**: The system creates stable "Plateaus" of operation rather than rapid flickering.
*   **Defense Value**: "I implemented standard Control Theory principles to ensure operational stability."

## 4. Detection Responsibility
**Requirement**: "Avoid circular logic where the Cloud waits for the Edge to detect a leak."
**Implementation Fix**:
*   **Separation of Duties**:
    *   *Baseline*: Cloud calculates `Flow vs Pressure`. (High Latency).
    *   *Edge*: Edge calculates `Flow vs Pressure`. (Low Latency).
*   **Result**: We can measure the time difference between `Edge Trigger` and `Cloud Trigger`.
*   **Defense Value**: "This allows me to quantify the 'Edge Advantage' (Latency Reduction) in milliseconds."

---

## 5. Defense Q&A Preparation

**Q: How do you know real detection isn't just random?**
**A:** "I transmit a 'Ground Truth' flag (`leak_flag`) from the simulator. I compare the System's Decision against this absolute truth to calculate False Positives/Negatives."

**Q: Why use Python for sensors? Isn't C++ better?**
**A:** "For a thesis proving *Architecture*, Python provides rapid prototyping of the logic. The bandwidth mechanics (MQTT packets) are identical regardless of the generating language."

**Q: What happens if the network cuts out?**
**A:** "The Edge Agent continues to run locally. Since it performs detection *before* transmission, it will still detect the leak (Local Survivability), satisfying the Safety requirement."
