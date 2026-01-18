# Agent Instructions (Enforced)

These rules are **authoritative** for all changes in this repo. If a user request conflicts with these rules, stop and ask for clarification.

## 1) System Model (Must Match)

The system is a **bidirectional feedback loop** with strict signal priority:

- **Bottom-up (edge → cloud):** `leak_detected` triggers higher fidelity.
- **Top-down (cloud → edge):** `memory_usage` and `cpu_usage` regulate data rate.

**Priority (strict):** `leak_detected` > `memory_usage` > `cpu_usage`.

**Why:**
- **Safety (Leak):** Must override all other constraints.
- **Survival (Memory):** Hard constraint; memory exhaustion can crash the system.
- **Optimization (CPU):** Soft constraint; high CPU is recoverable.

**Non-overrides:**
- `cpu_usage` and `memory_usage` must **never override** `leak_detected`.
- **Hysteresis/cooldown apply only to cpu/memory-driven decisions**, not leak escalation.

## 2) Mode Semantics (Fixed)

- `DEBUG` = 50 Hz passthrough
- `NORMAL` = 1 Hz aggregation
- `ECONOMY` = 1 / 5 min aggregation

## 3) Data Contract (Must Use Everywhere)

**Fields:**
- `pressure_psi`, `flow_gpm`, `tank_level_pct`
- `leak_truth`, `leak_detected`
- `mode_code` (0=NORMAL, 1=DEBUG, 2=ECONOMY)
- `staleness_s`

**Tags:**
- `site_id`, `scenario`, `run_id`, `sample_kind`

**Rules:**
- `leak_truth` is ground truth only (evaluation). **Never use for control.**
- Controller must react **only** to `leak_detected`.
- Always compute `staleness_s = cloud_receive_time - sensor_timestamp`.

## 4) Scenario Fairness (No Cheating)

- **Baseline (centralized):** detection happens in cloud logic (not truth).
- **Static/Adaptive:** detection happens at edge; cloud trusts `leak_detected`.
- Queries must be filtered by `run_id` to avoid stale-run triggers.

## 5) Memory Safety in ECONOMY

Do **not** buffer raw 50Hz for 5 minutes. Use **streaming aggregation** (running sums + max flags) to avoid memory spikes.

## 6) Decision Auditing (Required)

Every controller cycle must log: `cpu_pct`, `memory_pct`, `leak_detected`, `current_mode`, `target_mode`, `reason`, `cooldown_active` with `run_id`, `scenario`, `site_id` tags.

## 7) Deterministic Leak Schedule (Required for Tests)

Leaks must be deterministic and long enough to be observed by the controller (e.g., start T+60s, end T+180s).

## 8) Stop Conditions

If a task would violate any rule above (especially `leak_truth` usage), **stop and ask**.
