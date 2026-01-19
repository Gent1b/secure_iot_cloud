import argparse
import csv
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import requests


# --------------------------------------------------------------------------------------
# InfluxDB HTTP CSV querying (matches scripts/export_run_data.py style)
# --------------------------------------------------------------------------------------

def load_env_file(env_path: Path) -> Dict[str, str]:
    env: Dict[str, str] = {}
    if not env_path.exists():
        return env
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


@dataclass(frozen=True)
class InfluxConfig:
    url: str
    token: str
    org: str
    bucket: str


def load_influx_config(repo_root: Path) -> InfluxConfig:
    env = load_env_file(repo_root / ".env")

    url = os.getenv("INFLUXDB_URL", env.get("INFLUXDB_URL", "http://10.0.0.3:8086"))
    token = os.getenv("INFLUXDB_TOKEN", env.get("INFLUXDB_TOKEN", ""))
    org = os.getenv("INFLUXDB_ORG", env.get("INFLUXDB_ORG", ""))
    bucket = os.getenv("INFLUXDB_BUCKET", env.get("INFLUXDB_BUCKET", ""))

    return InfluxConfig(url=url, token=token, org=org, bucket=bucket)


def query_influx_csv(config: InfluxConfig, flux: str, timeout_s: int = 60) -> List[Dict[str, str]]:
    endpoint = f"{config.url.rstrip('/')}/api/v2/query?org={config.org}"
    headers = {
        "Authorization": f"Token {config.token}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv",
    }
    resp = requests.post(endpoint, headers=headers, data=flux.encode("utf-8"), timeout=timeout_s)
    resp.raise_for_status()

    # Influx CSV has comment lines starting with '#'
    lines = [line for line in resp.text.splitlines() if line and not line.startswith("#")]
    if not lines:
        return []

    reader = csv.DictReader(lines)
    return list(reader)


# --------------------------------------------------------------------------------------
# Flux query builders (always apply run_id + scenario filters)
# --------------------------------------------------------------------------------------

def _flux_time_literal(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt_utc = dt.astimezone(timezone.utc)
    # Flux supports time(v: "...")
    return f'time(v: "{dt_utc.isoformat().replace("+00:00", "Z")}")'


def _range_clause(*, since: Optional[str], start_time: Optional[datetime]) -> str:
    if start_time is not None:
        return f"  |> range(start: {_flux_time_literal(start_time)})\n"
    if not since:
        raise ValueError("either since or start_time must be provided")
    return f"  |> range(start: -{since})\n"


def flux_base(
    *,
    bucket: str,
    measurement: str,
    run_id: str,
    scenario: str,
    since: Optional[str],
    start_time: Optional[datetime] = None,
    site_id: Optional[str] = None,
) -> str:
    if not run_id:
        raise ValueError("run_id is required")
    if not scenario:
        raise ValueError("scenario is required")

    site_filter = f' and r.site_id == "{site_id}"' if site_id else ""

    return (
        f'from(bucket: "{bucket}")\n'
        f"{_range_clause(since=since, start_time=start_time)}"
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")\n'
        f'  |> filter(fn: (r) => r.run_id == "{run_id}" and r.scenario == "{scenario}"{site_filter})\n'
    )


def flux_count_points(
    *,
    bucket: str,
    measurement: str,
    field: str,
    run_id: str,
    scenario: str,
    since: str,
    site_id: Optional[str] = None,
    sample_kind: Optional[str] = None,
) -> str:
    kind_filter = f'  |> filter(fn: (r) => r.sample_kind == "{sample_kind}")\n' if sample_kind else ""

    return (
        flux_base(
            bucket=bucket,
            measurement=measurement,
            run_id=run_id,
            scenario=scenario,
            since=since,
            site_id=site_id,
        )
        + f'  |> filter(fn: (r) => r._field == "{field}")\n'
        + kind_filter
        + '  |> group()\n'
        + '  |> count(column: "_value")\n'
    )


def flux_first_time_for_field(
    *,
    bucket: str,
    measurement: str,
    field: str,
    run_id: str,
    scenario: str,
    since: Optional[str] = None,
    start_time: Optional[datetime] = None,
    site_id: Optional[str] = None,
    sample_kind: Optional[str] = None,
    value_equals: Optional[str] = None,
) -> str:
    kind_filter = f'  |> filter(fn: (r) => r.sample_kind == "{sample_kind}")\n' if sample_kind else ""
    value_filter = ""
    if value_equals is not None:
        # _value is numeric for int/float fields; use a string literal only for string fields.
        value_filter = f"  |> filter(fn: (r) => r._value == {value_equals})\n"

    return (
        flux_base(
            bucket=bucket,
            measurement=measurement,
            run_id=run_id,
            scenario=scenario,
            since=since,
            start_time=start_time,
            site_id=site_id,
        )
        + f'  |> filter(fn: (r) => r._field == "{field}")\n'
        + kind_filter
        + value_filter
        + '  |> group()\n'
        + '  |> keep(columns: ["_time"])\n'
        + '  |> sort(columns: ["_time"], desc: false)\n'
        + '  |> first()\n'
    )


def flux_last_time_for_field(
    *,
    bucket: str,
    measurement: str,
    field: str,
    run_id: str,
    scenario: str,
    since: str,
    site_id: Optional[str] = None,
    value_equals: Optional[str] = None,
) -> str:
    value_filter = f"  |> filter(fn: (r) => r._value == {value_equals})\n" if value_equals is not None else ""

    return (
        flux_base(
            bucket=bucket,
            measurement=measurement,
            run_id=run_id,
            scenario=scenario,
            since=since,
            site_id=site_id,
        )
        + f'  |> filter(fn: (r) => r._field == "{field}")\n'
        + value_filter
        + '  |> group()\n'
        + '  |> keep(columns: ["_time"])\n'
        + '  |> sort(columns: ["_time"], desc: false)\n'
        + '  |> last()\n'
    )


def flux_quantile(
    *,
    bucket: str,
    measurement: str,
    field: str,
    q: float,
    run_id: str,
    scenario: str,
    since: str,
    site_id: Optional[str] = None,
) -> str:
    # method estimate_tdigest is fast and robust for large series
    return (
        flux_base(
            bucket=bucket,
            measurement=measurement,
            run_id=run_id,
            scenario=scenario,
            since=since,
            site_id=site_id,
        )
        + f'  |> filter(fn: (r) => r._field == "{field}")\n'
        + '  |> group()\n'
        + f"  |> quantile(column: \"_value\", q: {q}, method: \"estimate_tdigest\")\n"
    )


def flux_controller_decision_time(
    *,
    bucket: str,
    run_id: str,
    scenario: str,
    since: Optional[str],
    start_time: Optional[datetime],
    site_id: Optional[str],
) -> str:
    return (
        flux_base(
            bucket=bucket,
            measurement="controller_audit",
            run_id=run_id,
            scenario=scenario,
            since=since,
            start_time=start_time,
            site_id=site_id,
        )
        + '  |> filter(fn: (r) => r._field == "target_mode" or r._field == "reason")\n'
        + '  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
        + '  |> filter(fn: (r) => r.target_mode == "DEBUG" and r.reason == "leak_priority")\n'
        + '  |> group()\n'
        + '  |> keep(columns: ["_time"])\n'
        + '  |> sort(columns: ["_time"], desc: false)\n'
        + '  |> first()\n'
    )


# --------------------------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------------------------

def parse_rfc3339(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        # Influx returns RFC3339 with Z
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def parse_float(value: Optional[str]) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def parse_int(value: Optional[str]) -> Optional[int]:
    if value is None or value == "":
        return None
    try:
        return int(float(value))
    except Exception:
        return None


def first_row(rows: List[Dict[str, str]]) -> Optional[Dict[str, str]]:
    return rows[0] if rows else None


def extract_time(rows: List[Dict[str, str]]) -> Optional[datetime]:
    row = first_row(rows)
    if not row:
        return None
    return parse_rfc3339(row.get("_time"))


def extract_value(rows: List[Dict[str, str]]) -> Optional[str]:
    row = first_row(rows)
    if not row:
        return None
    return row.get("_value")


# --------------------------------------------------------------------------------------
# Metric computation per run
# --------------------------------------------------------------------------------------


@dataclass
class RunSpec:
    scenario: str
    run_id: str


def parse_runs_arg(items: Sequence[str]) -> List[RunSpec]:
    runs: List[RunSpec] = []
    for item in items:
        if ":" not in item:
            raise ValueError(f"invalid --runs entry '{item}', expected SCENARIO:RUNID")
        scenario, run_id = item.split(":", 1)
        scenario = scenario.strip()
        run_id = run_id.strip()
        if not scenario or not run_id:
            raise ValueError(f"invalid --runs entry '{item}', expected SCENARIO:RUNID")
        runs.append(RunSpec(scenario=scenario, run_id=run_id))
    return runs


def compute_run_metrics(
    *,
    config: InfluxConfig,
    run: RunSpec,
    since: str,
    site_id: Optional[str],
) -> Dict[str, Any]:
    bucket = config.bucket
    scenario = run.scenario
    run_id = run.run_id

    # Counts (pressure_psi points)
    points_total = parse_int(
        extract_value(
            query_influx_csv(
                config,
                flux_count_points(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="pressure_psi",
                    run_id=run_id,
                    scenario=scenario,
                    since=since,
                    site_id=site_id,
                ),
            )
        )
    )

    points_raw = parse_int(
        extract_value(
            query_influx_csv(
                config,
                flux_count_points(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="pressure_psi",
                    run_id=run_id,
                    scenario=scenario,
                    since=since,
                    site_id=site_id,
                    sample_kind="raw",
                ),
            )
        )
    )

    points_agg = parse_int(
        extract_value(
            query_influx_csv(
                config,
                flux_count_points(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="pressure_psi",
                    run_id=run_id,
                    scenario=scenario,
                    since=since,
                    site_id=site_id,
                    sample_kind="agg",
                ),
            )
        )
    )

    # Duration based on first/last pressure_psi point
    first_time = extract_time(
        query_influx_csv(
            config,
            flux_first_time_for_field(
                bucket=bucket,
                measurement="water_pipeline",
                field="pressure_psi",
                run_id=run_id,
                scenario=scenario,
                since=since,
                site_id=site_id,
            ),
        )
    )

    last_time = extract_time(
        query_influx_csv(
            config,
            flux_last_time_for_field(
                bucket=bucket,
                measurement="water_pipeline",
                field="pressure_psi",
                run_id=run_id,
                scenario=scenario,
                since=since,
                site_id=site_id,
            ),
        )
    )

    duration_s: Optional[float] = None
    if first_time and last_time:
        duration_s = max(0.0, (last_time - first_time).total_seconds())

    points_per_s: Optional[float] = None
    if duration_s and duration_s > 0 and points_total is not None:
        points_per_s = points_total / duration_s

    # Staleness percentiles (staleness_s)
    staleness_p50 = parse_float(
        extract_value(
            query_influx_csv(
                config,
                flux_quantile(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="staleness_s",
                    q=0.5,
                    run_id=run_id,
                    scenario=scenario,
                    since=since,
                    site_id=site_id,
                ),
            )
        )
    )

    staleness_p95 = parse_float(
        extract_value(
            query_influx_csv(
                config,
                flux_quantile(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="staleness_s",
                    q=0.95,
                    run_id=run_id,
                    scenario=scenario,
                    since=since,
                    site_id=site_id,
                ),
            )
        )
    )

    # Leak window from leak_truth == 1 (evaluation only)
    leak_start = extract_time(
        query_influx_csv(
            config,
            flux_first_time_for_field(
                bucket=bucket,
                measurement="water_pipeline",
                field="leak_truth",
                run_id=run_id,
                scenario=scenario,
                since=since,
                site_id=site_id,
                value_equals="1",
            ),
        )
    )

    leak_end = extract_time(
        query_influx_csv(
            config,
            flux_last_time_for_field(
                bucket=bucket,
                measurement="water_pipeline",
                field="leak_truth",
                run_id=run_id,
                scenario=scenario,
                since=since,
                site_id=site_id,
                value_equals="1",
            ),
        )
    )

    # Detection time from leak_detected == 1 after leak_start
    detection_time = extract_time(
        query_influx_csv(
            config,
            flux_first_time_for_field(
                bucket=bucket,
                measurement="water_pipeline",
                field="leak_detected",
                run_id=run_id,
                scenario=scenario,
                since=None if leak_start else since,
                start_time=leak_start,
                site_id=site_id,
                value_equals="1",
            ),
        )
    )

    # Controller decision time (audit) after leak_start (or within since window)
    controller_decision_time = extract_time(
        query_influx_csv(
            config,
            flux_controller_decision_time(
                bucket=bucket,
                run_id=run_id,
                scenario=scenario,
                since=None if leak_start else since,
                start_time=leak_start,
                site_id=site_id,
            ),
        )
    )

    # First raw sample after controller decision (pressure_psi, sample_kind raw)
    first_raw_after_decision = None
    if controller_decision_time is not None:
        first_raw_after_decision = extract_time(
            query_influx_csv(
                config,
                flux_first_time_for_field(
                    bucket=bucket,
                    measurement="water_pipeline",
                    field="pressure_psi",
                    run_id=run_id,
                    scenario=scenario,
                    since=None,
                    start_time=controller_decision_time,
                    site_id=site_id,
                    sample_kind="raw",
                ),
            )
        )

    # Derived deltas
    detection_latency_s: Optional[float] = None
    decision_latency_s: Optional[float] = None
    decision_to_first_raw_s: Optional[float] = None

    if leak_start and detection_time:
        detection_latency_s = max(0.0, (detection_time - leak_start).total_seconds())
    if leak_start and controller_decision_time:
        decision_latency_s = max(0.0, (controller_decision_time - leak_start).total_seconds())
    if controller_decision_time and first_raw_after_decision:
        decision_to_first_raw_s = max(0.0, (first_raw_after_decision - controller_decision_time).total_seconds())

    def fmt(dt: Optional[datetime]) -> Optional[str]:
        if dt is None:
            return None
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    return {
        "run_id": run_id,
        "scenario": scenario,
        "site_id": site_id,
        "since": since,
        "points_total": points_total,
        "points_raw": points_raw,
        "points_agg": points_agg,
        "duration_s": duration_s,
        "points_per_s": points_per_s,
        "staleness_p50": staleness_p50,
        "staleness_p95": staleness_p95,
        "leak_start": fmt(leak_start),
        "leak_end": fmt(leak_end),
        "detection_time": fmt(detection_time),
        "controller_decision_time": fmt(controller_decision_time),
        "first_raw_after_decision": fmt(first_raw_after_decision),
        "detection_latency_s": detection_latency_s,
        "decision_latency_s": decision_latency_s,
        "decision_to_first_raw_s": decision_to_first_raw_s,
    }


def write_json(obj: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True), encoding="utf-8")


def write_csv_row(path: Path, row: Dict[str, Any], columns: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=columns)
        w.writeheader()
        w.writerow({c: row.get(c, "") for c in columns})


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Evaluate one or more experimental runs by querying InfluxDB 2.x via /api/v2/query (CSV)."
        )
    )
    parser.add_argument(
        "--runs",
        nargs="+",
        required=True,
        help="Runs to evaluate as SCENARIO:RUNID (e.g., baseline:baseline-20260101-120000 static:static-... adaptive:adaptive-...)",
    )
    parser.add_argument("--since", default="30m", help="Time range (e.g., 30m, 2h, 1d)")
    parser.add_argument("--site-id", default=None, help="Optional site_id filter (e.g., plant-a)")
    parser.add_argument("--out-dir", default="exports", help="Base output directory")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    config = load_influx_config(repo_root)

    if not config.token or not config.org or not config.bucket:
        print("Missing InfluxDB configuration (INFLUXDB_TOKEN/INFLUXDB_ORG/INFLUXDB_BUCKET).", file=sys.stderr)
        return 1

    try:
        runs = parse_runs_arg(args.runs)
    except ValueError as e:
        print(str(e), file=sys.stderr)
        return 2

    ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    exit_code = 0
    for run in runs:
        try:
            metrics = compute_run_metrics(
                config=config,
                run=run,
                since=args.since,
                site_id=args.site_id,
            )

            out_dir = Path(args.out_dir) / run.run_id
            json_path = out_dir / f"metrics_{ts}.json"
            csv_path = out_dir / f"metrics_{ts}.csv"

            write_json(metrics, json_path)
            write_csv_row(
                csv_path,
                metrics,
                columns=[
                    "run_id",
                    "scenario",
                    "site_id",
                    "since",
                    "points_total",
                    "points_raw",
                    "points_agg",
                    "duration_s",
                    "points_per_s",
                    "staleness_p50",
                    "staleness_p95",
                    "leak_start",
                    "leak_end",
                    "detection_time",
                    "controller_decision_time",
                    "first_raw_after_decision",
                    "detection_latency_s",
                    "decision_latency_s",
                    "decision_to_first_raw_s",
                ],
            )

            print(f"Wrote {json_path} and {csv_path}")
        except requests.HTTPError as e:
            exit_code = 3
            print(f"InfluxDB query failed for {run.scenario}:{run.run_id}: {e}", file=sys.stderr)
        except Exception as e:
            exit_code = 4
            print(f"Failed to evaluate {run.scenario}:{run.run_id}: {e}", file=sys.stderr)

    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
