# Feedback Loop Rules (Authoritative)

This project is modeled as a **bidirectional feedback loop** with strict signal priority.

## Directions

1) **Bottom-up (edge → cloud):** `leak_detected` forces higher fidelity (DEBUG).
2) **Top-down (cloud → edge):** `memory_usage` and `cpu_usage` regulate data rate.

## Priority (Strict)

`leak_detected` > `memory_usage` > `cpu_usage`

## Why

- **Safety (Leak):** Critical events must never be missed.
- **Survival (Memory):** Memory exhaustion can crash services; hard constraint.
- **Optimization (CPU):** High CPU is recoverable; soft constraint.

## Non-overrides

- `cpu_usage` and `memory_usage` **must never override** `leak_detected`.
- **Hysteresis/cooldown apply only to cpu/memory-driven decisions.**

## Decision Rules (Summary)

- If `leak_detected == 1` → `DEBUG` immediately (no cooldown/hysteresis).
- Else if `memory_usage >= MEM_HIGH` → `ECONOMY` (hard constraint).
- Else if `cpu_usage >= CPU_HIGH` → `ECONOMY` (soft constraint).
- Else if `cpu_usage <= CPU_LOW` and `memory_usage <= MEM_LOW` → optional `DEBUG` (opportunistic).
- Else → `NORMAL`.
