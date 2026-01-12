import os
import time
import json
import logging
import requests
import paho.mqtt.client as mqtt
from collections import deque
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WritePrecision, SYNCHRONOUS
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
write_api = influx_client.write_api(write_options=SYNCHRONOUS)

# ---------------------------------------------------------------------
# THESIS FIX #5: Controller State for Stability
# ---------------------------------------------------------------------
CURRENT_MODES = {site: "NORMAL" for site in SITES}
LAST_MODE_CHANGE = {site: 0.0 for site in SITES}
CPU_HISTORY = deque(maxlen=3)  # 3 samples = 30 seconds of history
COOLDOWN_SECONDS = 30  # Minimum time between mode changes

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
    THESIS FIX #5: Includes hysteresis and cooldown to prevent oscillation.
    """
    # Update CPU history for smoothing
    CPU_HISTORY.append(cloud_cpu)
    avg_cpu = sum(CPU_HISTORY) / len(CPU_HISTORY)
    
    commands = {}
    current_time = time.time()
    
    leaking_sites = [s for s, state in site_states.items() if state == "LEAK_DETECTED"]
    
    # SCENARIO 1: CRITICAL EVENT (Leak) - ALWAYS takes priority
    if leaking_sites:
        log.info(f"SCENARIO: CRITICAL LEAK DETECTED at {leaking_sites}. avg_cpu={avg_cpu:.1f}%")
        for site in SITES:
            if site in leaking_sites:
                new_mode = "DEBUG"   # 50Hz Raw Data
                reason = "leak_priority"
            else:
                new_mode = "ECONOMY" # Throttle to save bandwidth for the leak
                reason = "leak_throttle"
            
            # Apply with cooldown check
            if new_mode != CURRENT_MODES[site]:
                if current_time - LAST_MODE_CHANGE[site] >= COOLDOWN_SECONDS:
                    CURRENT_MODES[site] = new_mode
                    LAST_MODE_CHANGE[site] = current_time
                    DECISION_COUNTER.labels(site_id=site, target_mode=new_mode, reason=reason).inc()
                else:
                    # Still in cooldown, keep current mode
                    new_mode = CURRENT_MODES[site]
            
            commands[site] = new_mode
                
    # SCENARIO 2: CLOUD CONGESTION (High CPU) with Hysteresis
    elif avg_cpu > 80.0:
        log.info(f"SCENARIO: CLOUD CONGESTION (avg_cpu={avg_cpu:.1f}%). Throttling all sites.")
        for site in SITES:
            new_mode = "ECONOMY"
            reason = "congestion_control"
            
            if new_mode != CURRENT_MODES[site]:
                if current_time - LAST_MODE_CHANGE[site] >= COOLDOWN_SECONDS:
                    CURRENT_MODES[site] = new_mode
                    LAST_MODE_CHANGE[site] = current_time
                    DECISION_COUNTER.labels(site_id=site, target_mode=new_mode, reason=reason).inc()
                else:
                    new_mode = CURRENT_MODES[site]
            
            commands[site] = new_mode
            
    # SCENARIO 3: CLOUD IDLE (Resource Maximization) with Exit Hysteresis
    elif avg_cpu < 20.0:
        log.info(f"SCENARIO: CLOUD IDLE (avg_cpu={avg_cpu:.1f}%). Requesting High-Fidelity Data.")
        for site in SITES:
            new_mode = "DEBUG" # Send everything! We have space.
            reason = "idle_utilization"
            
            if new_mode != CURRENT_MODES[site]:
                if current_time - LAST_MODE_CHANGE[site] >= COOLDOWN_SECONDS:
                    CURRENT_MODES[site] = new_mode
                    LAST_MODE_CHANGE[site] = current_time
                    DECISION_COUNTER.labels(site_id=site, target_mode=new_mode, reason=reason).inc()
                else:
                    new_mode = CURRENT_MODES[site]
            
            commands[site] = new_mode
            
    # SCENARIO 4: NORMAL OPERATION (20% < CPU < 80%)
    else:
        log.info(f"SCENARIO: NORMAL OPERATION (avg_cpu={avg_cpu:.1f}%).")
        for site in SITES:
            # Hysteresis: Stay in ECONOMY if between 60-80%, switch to NORMAL only below 60%
            if CURRENT_MODES[site] == "ECONOMY" and avg_cpu > 60.0:
                new_mode = "ECONOMY"  # Stay in economy until clearly below threshold
                reason = "hysteresis_economy"
            else:
                new_mode = "NORMAL"
                reason = "normal_operation"
            
            if new_mode != CURRENT_MODES[site]:
                if current_time - LAST_MODE_CHANGE[site] >= COOLDOWN_SECONDS:
                    CURRENT_MODES[site] = new_mode
                    LAST_MODE_CHANGE[site] = current_time
                    DECISION_COUNTER.labels(site_id=site, target_mode=new_mode, reason=reason).inc()
                else:
                    new_mode = CURRENT_MODES[site]
            
            commands[site] = new_mode
            
    return commands

def send_commands(commands):
    """
    Publishes mode commands via MQTT and persists decisions to InfluxDB.
    THESIS FIX #7: Traceability of controller decisions.
    """
    for site, mode in commands.items():
        topic = f"{TOPIC_CONTROL}/{site}"
        payload = json.dumps({"mode": mode, "timestamp": time.time()})
        mqtt_client.publish(topic, payload, qos=1)
        
        # THESIS FIX #7: Persist decision event to InfluxDB
        try:
            point = Point("controller_decisions") \
                .tag("site_id", site) \
                .field("target_mode", mode) \
                .field("decision_timestamp", time.time())
            write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=point)
        except Exception as e:
            log.error(f"Failed to persist decision for {site}: {e}")

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
