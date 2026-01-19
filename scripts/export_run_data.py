import argparse
import csv
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional

import requests


def load_env_file(env_path: Path) -> Dict[str, str]:
    env = {}
    if not env_path.exists():
        return env
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        env[key.strip()] = value.strip()
    return env


def flux_query(bucket: str, run_id: str, scenario: Optional[str], since: str, fields: List[str], measurement: str) -> str:
    field_filter = " or ".join([f'r._field == "{f}"' for f in fields])
    scenario_filter = f' and r.scenario == "{scenario}"' if scenario else ""

    return (
        f'from(bucket: "{bucket}")\n'
        f'  |> range(start: -{since})\n'
        f'  |> filter(fn: (r) => r._measurement == "{measurement}")\n'
        f'  |> filter(fn: (r) => r.run_id == "{run_id}"{scenario_filter})\n'
        f'  |> filter(fn: (r) => {field_filter})\n'
        f'  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")\n'
        f'  |> sort(columns: ["_time"], desc: false)\n'
    )


def write_csv(rows: Iterable[Dict[str, str]], out_path: Path, columns: List[str]) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: row.get(k, "") for k in columns})


def query_influx(url: str, token: str, org: str, flux: str) -> List[Dict[str, str]]:
    endpoint = f"{url.rstrip('/')}/api/v2/query?org={org}"
    headers = {
        "Authorization": f"Token {token}",
        "Content-Type": "application/vnd.flux",
        "Accept": "application/csv",
    }
    resp = requests.post(endpoint, headers=headers, data=flux.encode("utf-8"), timeout=60)
    resp.raise_for_status()

    lines = [line for line in resp.text.splitlines() if line and not line.startswith("#")]
    if not lines:
        return []

    reader = csv.DictReader(lines)
    return list(reader)


def main() -> int:
    parser = argparse.ArgumentParser(description="Export thesis run data from InfluxDB")
    parser.add_argument("--run-id", default=None, help="Run ID to export (default: RUN_ID env or run_unknown)")
    parser.add_argument("--scenario", default=None, help="Scenario filter (baseline|static|adaptive)")
    parser.add_argument("--since", default="20m", help="Time range (e.g., 20m, 2h, 1d)")
    parser.add_argument("--out-dir", default="exports", help="Output directory")
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parents[1]
    env = load_env_file(repo_root / ".env")

    influx_url = os.getenv("INFLUXDB_URL", env.get("INFLUXDB_URL", "http://10.0.0.3:8086"))
    influx_token = os.getenv("INFLUXDB_TOKEN", env.get("INFLUXDB_TOKEN", ""))
    influx_org = os.getenv("INFLUXDB_ORG", env.get("INFLUXDB_ORG", ""))
    influx_bucket = os.getenv("INFLUXDB_BUCKET", env.get("INFLUXDB_BUCKET", ""))

    run_id = args.run_id or os.getenv("RUN_ID", "run_unknown")
    scenario = args.scenario

    if not influx_token or not influx_org or not influx_bucket:
        print("Missing InfluxDB configuration (TOKEN/ORG/BUCKET).", file=sys.stderr)
        return 1

    out_dir = Path(args.out_dir) / run_id
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")

    # Water pipeline data
    water_fields = [
        "pressure_psi",
        "flow_gpm",
        "tank_level_pct",
        "leak_truth",
        "leak_detected",
        "mode_code",
        "staleness_s",
    ]
    water_flux = flux_query(influx_bucket, run_id, scenario, args.since, water_fields, "water_pipeline")
    water_rows = query_influx(influx_url, influx_token, influx_org, water_flux)

    water_columns = [
        "_time",
        "device_id",
        "site_id",
        "scenario",
        "run_id",
        "sample_kind",
    ] + water_fields
    write_csv(water_rows, out_dir / f"water_pipeline_{timestamp}.csv", water_columns)

    # Controller audit
    audit_fields = [
        "cpu_pct",
        "memory_pct",
        "leak_detected",
        "current_mode",
        "target_mode",
        "reason",
        "cooldown_active",
    ]
    audit_flux = flux_query(influx_bucket, run_id, scenario, args.since, audit_fields, "controller_audit")
    audit_rows = query_influx(influx_url, influx_token, influx_org, audit_flux)

    audit_columns = [
        "_time",
        "site_id",
        "scenario",
        "run_id",
        "sample_kind",
    ] + audit_fields
    write_csv(audit_rows, out_dir / f"controller_audit_{timestamp}.csv", audit_columns)

    # Controller decisions
    decision_fields = [
        "target_mode",
        "decision_timestamp",
    ]
    decision_flux = flux_query(influx_bucket, run_id, scenario, args.since, decision_fields, "controller_decisions")
    decision_rows = query_influx(influx_url, influx_token, influx_org, decision_flux)

    decision_columns = [
        "_time",
        "site_id",
        "scenario",
        "run_id",
        "sample_kind",
    ] + decision_fields
    write_csv(decision_rows, out_dir / f"controller_decisions_{timestamp}.csv", decision_columns)

    print(f"Export complete: {out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
