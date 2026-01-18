# Data Contract (Canonical)

This file is the **single source of truth** for all fields, tags, and enums used in telemetry, control, and audit logs.

## Fields (Measurements)

- `pressure_psi`
- `flow_gpm`
- `tank_level_pct`
- `leak_truth` (0/1, simulation ground truth only)
- `leak_detected` (0/1, algorithm output)
- `mode_code` (0=NORMAL, 1=DEBUG, 2=ECONOMY)
- `staleness_s` (seconds, cloud_receive_time - sensor_timestamp)

## Tags (Every point/log line must include)

- `site_id` (e.g., plant-a)
- `scenario` (baseline | static | adaptive)
- `run_id` (unique per run)
- `sample_kind` (raw | agg | decision | status)

## Rules

- `leak_truth` is **never** used for control decisions.
- Controller must react **only** to `leak_detected`.
- Always include `run_id` in queries to avoid stale-run triggers.
- `mode_code` must be stable across all components.
