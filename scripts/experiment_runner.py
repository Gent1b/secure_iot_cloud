import argparse
import time
import json
import os
import subprocess
import datetime
from pathlib import Path

# ==============================================================================
# CONFIGURATION
# ==============================================================================
SSH_USER = "root"
NODES = {
    "monitor": "monitor",  # Hostname in SSH config or IP
    "cloud": "cloud",
    "devices": "devices"
}

# Metrics to Export from Prometheus
PROMETHEUS_QUERIES = {
    "bandwidth_total": "iot_bandwidth_bytes_total",
    "bandwidth_rate_1m": "rate(iot_bandwidth_bytes_total[1m])",
    "latency_e2e_avg_1m": "rate(iot_end_to_end_latency_seconds_sum[1m]) / rate(iot_end_to_end_latency_seconds_count[1m])",
    "processing_latency_avg_1m": "rate(iot_processing_latency_seconds_sum[1m]) / rate(iot_processing_latency_seconds_count[1m])",
    "edge_mode": "edge_operating_mode",
    "influx_writes_total": "influxdb_writes_success_total",
    "influx_writes_rate_1m": "rate(influxdb_writes_success_total[1m])",
    "cloud_cpu_usage": "100 - (avg by (instance) (rate(node_cpu_seconds_total{mode='idle'}[1m])) * 100)",
    "leaks_detected_total": "edge_leaks_detected_total",
    "container_cpu": "sum by (container_name) (rate(docker_container_cpu_usage_total[1m]))"
}

# ==============================================================================
# UTILS
# ==============================================================================
def run_ssh_command(host, command):
    """Run a command on a remote host via SSH."""
    full_cmd = ["ssh", host, command]
    print(f"DEBUG: Running {' '.join(full_cmd)}")
    try:
        result = subprocess.run(full_cmd, capture_output=True, text=True, check=True)
        return result.stdout.strip()
    except subprocess.CalledProcessError as e:
        print(f"ERROR executing on {host}: {e}")
        print(f"stderr: {e.stderr}")
        return None

def fetch_prometheus_data(query, start_ts, end_ts, step="5s"):
    """Fetch range data from Prometheus via curl on the monitoring node."""
    # Construct URL parameters
    # URL: http://localhost:9090/api/v1/query_range?query=...&start=...&end=...&step=...
    
    # We use python's urllib logic to quote, but since we are passing to shell, 
    # we need to be careful. Simple replace for spaces to %20 is usually enough for simple queries.
    encoded_query = query.replace(" ", "%20").replace("'", "%27").replace('"', "%22")
    
    url = f"http://localhost:9090/api/v1/query_range?query={encoded_query}&start={start_ts}&end={end_ts}&step={step}"
    
    cmd = f"curl -s -g '{url}'"
    json_str = run_ssh_command(NODES["monitor"], cmd)
    
    if json_str:
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            print(f"Failed to decode JSON from Prometheus for {query}")
            return None
    return None

def fetch_logs(node, container_selector, tail=2000):
    """Fetch logs from Docker or K3s."""
    # Try Docker first
    cmd = f"docker logs {container_selector} --tail {tail} 2>&1"
    
    # Heuristic for Edge Node (K3s) vs Docker
    if node == "devices" and "app=" in container_selector:
         cmd = f"kubectl logs -n default -l {container_selector} --tail {tail}"
         
    return run_ssh_command(NODES[node], cmd)

# ==============================================================================
# MAIN RUNNER
# ==============================================================================
def run_experiment(scenario_name, duration_seconds):
    start_time = datetime.datetime.utcnow()
    start_ts = int(time.time())
    
    print(f"=======================================================")
    print(f" STARTING EXPERIMENT: {scenario_name}")
    print(f" Duration: {duration_seconds} seconds")
    print(f" Start Time (UTC): {start_time}")
    print(f"=======================================================")
    
    # Wait for experiment duration
    try:
        for remaining in range(duration_seconds, 0, -10):
            print(f"Experiment running... {remaining}s remaining")
            time.sleep(10)
    except KeyboardInterrupt:
        print("\nExperiment interrupted! Collecting data captured so far...")

    end_time = datetime.datetime.utcnow()
    end_ts = int(time.time())
    
    # Create Output Directory
    timestamp_str = start_time.strftime("%Y%m%d_%H%M%S")
    out_dir = Path(f"evaluation_data/{scenario_name}/{timestamp_str}")
    out_dir.mkdir(parents=True, exist_ok=True)
    
    (out_dir / "prometheus_exports").mkdir()
    (out_dir / "logs").mkdir()
    
    print(f"\n>>> Collecting Data into {out_dir}...")
    
    # 1. Export Prometheus Metrics
    print(">>> Exporting Prometheus Metrics...")
    for name, query in PROMETHEUS_QUERIES.items():
        data = fetch_prometheus_data(query, start_ts, end_ts)
        if data:
            with open(out_dir / f"prometheus_exports/{name}.json", "w") as f:
                json.dump(data, f, indent=2)
        else:
            print(f"WARNING: No data for {name}")

    # 2. Export Logs
    print(">>> Exporting Logs...")
    
    # Controller Logs (Cloud)
    ctl_logs = fetch_logs("cloud", "cloud-node-controller-1") # Docker Compose name guess
    # Fallback to just 'controller' if container name varies
    if not ctl_logs or "No such container" in ctl_logs:
        ctl_logs = fetch_logs("cloud", "controller")
    
    with open(out_dir / "logs/cloud_controller.log", "w", encoding="utf-8") as f:
        f.write(ctl_logs if ctl_logs else "No logs found")

    # Edge Logs (Devices)
    # Check if we are in K3s mode (Static/Dynamic) or Docker (Baseline)
    # Try K3s first
    edge_logs = fetch_logs("devices", "app=edge-agent")
    if not edge_logs or "api not found" in edge_logs.lower():
         # Fallback to Docker Publisher/Subscriber
         edge_logs = fetch_logs("devices", "iot-publisher") # Baseline publisher
         
    with open(out_dir / "logs/edge_nodes.log", "w", encoding="utf-8") as f:
        f.write(edge_logs if edge_logs else "No logs found")

    # 3. Metadata
    metadata = {
        "scenario": scenario_name,
        "start_time_utc": str(start_time),
        "end_time_utc": str(end_time),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "config": {
            "aggregation_window_normal": 60,
            "aggregation_window_economy": 300,
            "controller_interval": 10,
            "threshold_cpu_high": 80.0,
            "threshold_cpu_low": 20.0 # FIXED
        },
        "notes": "Generated by Automated Experiment Runner"
    }
    
    with open(out_dir / "metadata.json", "w") as f:
        json.dump(metadata, f, indent=2)

    # 4. Generate README
    with open(out_dir / "README.md", "w") as f:
        f.write(f"# Experiment Data: {scenario_name}\n\n")
        f.write(f"**Date:** {start_time}\n")
        f.write(f"**Duration:** {duration_seconds}s\n\n")
        f.write("## Contents\n")
        f.write("- `prometheus_exports/`: JSON dumps of PromQL queries.\n")
        f.write("- `logs/`: System logs from Controller and Edge.\n")
        f.write("- `metadata.json`: Experiment context.\n")

    print(f"\nSUCCESS: Experiment Complete. Data saved to {out_dir}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Thesis Experiment Runner")
    parser.add_argument("scenario", help="Name of the scenario (baseline, static, dynamic)")
    parser.add_argument("--duration", type=int, default=300, help="Duration in seconds (default: 300)")
    args = parser.parse_args()
    
    run_experiment(args.scenario, args.duration)
