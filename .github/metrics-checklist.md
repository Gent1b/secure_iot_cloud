# Metrics Checklist (Minimum)

These metrics must exist for evaluation and future dashboards.

## Cost

- Total points written per run
- Average points/sec per run
- Optional: total bytes per run

## Freshness (Aral axis)

- `staleness_s` distribution (p50, p95)
- `staleness_s` over time (especially during leak windows)

## Event Fidelity

- High-rate points received within leak window (e.g., ±10s or ±30s)
- Pressure trace around leak (baseline vs static vs adaptive)

## Latency Chain

- Leak start (`leak_truth`)
- Detection (`leak_detected` first 1)
- Controller decision (first DEBUG)
- Edge apply/ACK
- First high-rate point visible in cloud

## Auditability

- Each controller cycle records `reason` and `cooldown_active`
- Queries filter by `run_id` to avoid stale-run contamination
