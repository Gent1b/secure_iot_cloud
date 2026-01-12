import os
import time
import json
import logging
import requests
import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient
from prometheus_client import start_http_server, Counter

# ---------------------------------------------------------------------
# Metrics
# ---------------------------------------------------------------------
DECISION_COUNTER = Counter(
    'controller_decisions_total',
    'Number of control feedback decisions made',
    ['site_id', 'target_mode', 'reason']
)

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BROKER = os.getenv("BROKER", "mqtt_broker")
PORT = 1883
TOPIC_CONTROL = "iot/control"

INFLUX_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUX_TOKEN = os.getenv("INFLUXDB_TOKEN", "local_token_123")
INFLUX_ORG = os.getenv("INFLUXDB_ORG", "secure_iot")
INFLUX_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://10.0.0.5:9090")

# Sites to manage
SITES = ["plant-a", "plant-b", "plant-c"]

# ---------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [CONTROLLER] %(message)s")
log = logging.getLogger("controller")

# ---------------------------------------------------------------------
# InfluxDB Client
# ---------------------------------------------------------------------
influx_client = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
query_api = influx_client.query_api()

# ---------------------------------------------------------------------
# MQTT Client
# ---------------------------------------------------------------------
mqtt_client = mqtt.Client(client_id="cloud_controller")

def connect_mqtt():
    while True:
        try:
            mqtt_client.connect(BROKER, PORT, 60)
            mqtt_client.loop_start()
            log.info("Connected to MQTT Broker.")
            return
        except Exception as e:
            log.error(f"MQTT Connection failed: {e}. Retrying...")
            time.sleep(5)

# ---------------------------------------------------------------------
# Logic: Fairness Algorithm
# ---------------------------------------------------------------------
def check_system_state():
    """
    Queries InfluxDB to determine the state of each site and the cloud.
    """
    site_states = {site: "NORMAL" for site in SITES}
    
    # 1. Check for Leaks (Last 1 minute)
    # We look for any record where leak_flag == 1
    query = f'''
    from(bucket: "{INFLUX_BUCKET}")
      |> range(start: -1m)
      |> filter(fn: (r) => r["_measurement"] == "water_pipeline")
      |> filter(fn: (r) => r["_field"] == "leak_flag")
      |> filter(fn: (r) => r["_value"] == 1)
      |> keep(columns: ["site_id"])
      |> distinct(column: "site_id")
    '''
    
    try:
        tables = query_api.query(query)
        for table in tables:
            for record in table.records:
                site = record["site_id"]
                if site in site_states:
                    site_states[site] = "LEAK_DETECTED"
                    log.warning(f"Leak detected at {site}!")
    except Exception as e:
        log.error(f"InfluxDB Query Error: {e}")

    # 2. Check Cloud Load (Real CPU Usage from Prometheus/Node Exporter)
    # Query Prometheus for CPU usage on cloud node
    # Query: 100 - (avg(rate(node_cpu_seconds_total{job="cloud_node_exporter",mode="idle"}[1m])) * 100)
    
    cloud_cpu_usage = 15.0  # Default fallback
    try:
        query = '100 - (avg(rate(node_cpu_seconds_total{job="cloud_node_exporter",mode="idle"}[1m])) * 100)'
        response = requests.get(
            f"{PROMETHEUS_URL}/api/v1/query",
            params={'query': query},
            timeout=5
        )
        
        if response.status_code == 200:
            data = response.json()
            if data['status'] == 'success' and data['data']['result']:
                cloud_cpu_usage = float(data['data']['result'][0]['value'][1])
                log.debug(f"Cloud CPU from Prometheus: {cloud_cpu_usage:.2f}%")
            else:
                log.warning("Prometheus query returned no results, using default CPU")
        else:
            log.warning(f"Prometheus returned status {response.status_code}, using default CPU")
            
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to query Prometheus: {e}. Using default CPU value.")
    except (KeyError, IndexError, ValueError) as e:
        log.error(f"Failed to parse Prometheus response: {e}. Using default CPU value.")

    return site_states, cloud_cpu_usage

def enforce_fairness(site_states, cloud_cpu):
    """
    Decides the mode for each site based on states and cloud capacity.
    """
    commands = {}
    
    leaking_sites = [s for s, state in site_states.items() if state == "LEAK_DETECTED"]
    
    # SCENARIO 1: CRITICAL EVENT (Leak)
    if leaking_sites:
        log.info(f"SCENARIO: CRITICAL LEAK DETECTED. Prioritizing {leaking_sites}")
        for site in SITES:
            if site in leaking_sites:
                commands[site] = "DEBUG"   # 50Hz Raw Data
                DECISION_COUNTER.labels(site_id=site, target_mode="DEBUG", reason="leak_priority").inc()
            else:
                commands[site] = "ECONOMY" # Throttle to save bandwidth for the leak
                DECISION_COUNTER.labels(site_id=site, target_mode="ECONOMY", reason="leak_throttle").inc()
                
    # SCENARIO 2: CLOUD CONGESTION (High CPU)
    elif cloud_cpu > 80.0:
        log.info(f"SCENARIO: CLOUD CONGESTION (CPU {cloud_cpu}%). Throttling all sites.")
        for site in SITES:
            commands[site] = "ECONOMY"
            DECISION_COUNTER.labels(site_id=site, target_mode="ECONOMY", reason="congestion_control").inc()
            
    # SCENARIO 3: CLOUD IDLE (Resource Maximization)
    elif cloud_cpu < 20.0:
        log.info(f"SCENARIO: CLOUD IDLE (CPU {cloud_cpu}%). Requesting High-Fidelity Data.")
        for site in SITES:
            commands[site] = "DEBUG" # Send everything! We have space.
            DECISION_COUNTER.labels(site_id=site, target_mode="DEBUG", reason="idle_utilization").inc()
            
    # SCENARIO 4: NORMAL OPERATION
    else:
        log.info(f"SCENARIO: NORMAL OPERATION (CPU {cloud_cpu}%).")
        for site in SITES:
            commands[site] = "NORMAL"
            DECISION_COUNTER.labels(site_id=site, target_mode="NORMAL", reason="normal_operation").inc()
            
    return commands

def send_commands(commands):
    for site, mode in commands.items():
        topic = f"{TOPIC_CONTROL}/{site}"
        payload = json.dumps({"mode": mode, "timestamp": time.time()})
        mqtt_client.publish(topic, payload, qos=1)
        # log.info(f"Sent command to {site}: {mode}") # Reduce log noise

# ---------------------------------------------------------------------
# Main Loop
# ---------------------------------------------------------------------
def main():
    # Start Prometheus Metrics Server
    start_http_server(8000)
    log.info("Metrics server started on port 8000")

    connect_mqtt()
    
    log.info("Starting Feedback Control Loop...")
    
    while True:
        try:
            # 1. Sense
            states, cpu_load = check_system_state()
            
            # 2. Plan
            decisions = enforce_fairness(states, cpu_load)
            
            # 3. Act
            send_commands(decisions)
            
        except Exception as e:
            log.error(f"Control Loop Error: {e}")
            
        time.sleep(10) # Run every 10 seconds

if __name__ == "__main__":
    main()
