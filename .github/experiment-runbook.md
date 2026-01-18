# Experiment Runbook (Baseline / Static / Adaptive)

This runbook defines the **minimum steps** to produce comparable runs with consistent leak timing and tags.

## Pre-Run

- Set `RUN_ID` (unique string, e.g., timestamp).
- Set `SCENARIO` to one of: `baseline`, `static`, `adaptive`.
- Ensure deterministic leak schedule is enabled (e.g., T+60s to T+180s).

## Scenario A: Baseline (Centralized)

- Cloud receives full-rate data (50 Hz).
- Detection is computed in the cloud (not `leak_truth`).
- No adaptive throttling.

## Scenario B: Static Edge

- Edge detects leaks locally and sends aggregated data at `NORMAL` rate only.
- No mode switching.

## Scenario C: Adaptive Edge

- Edge detects leaks locally.
- Controller enforces priority order and switches modes.
- Hysteresis/cooldown apply only to cpu/memory-driven transitions.

## Required Outputs

- InfluxDB points tagged with `run_id`, `scenario`, `site_id`, `sample_kind`.
- Controller audit logs (`decision`/`reason`/`cooldown_active`).
- `staleness_s` recorded for all data points.
