import os
import json
import time
import logging
import queue
from collections import defaultdict, deque
from threading import Thread

import paho.mqtt.client as mqtt
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import WritePrecision, SYNCHRONOUS
from prometheus_client import start_http_server, Counter, Gauge

# ---------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------
BROKER = os.getenv("BROKER", "mqtt_broker")
TOPIC = os.getenv("MQTT_TOPIC", "iot/devices")
TOPIC_PROCESSED = os.getenv("MQTT_TOPIC_PROCESSED", "iot/data/processed")

# InfluxDB (Cloud Mode)
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")

# Operation Modes
# CLOUD: Write to InfluxDB directly.
# EDGE: Validate, Aggregate, Detect Leaks, then Publish to 'processed' topic.
MODE = os.getenv("MODE", "CLOUD")  # CLOUD or EDGE

# Tunables
SUBSCRIBER_QUEUE_MAX = int(os.getenv("SUBSCRIBER_QUEUE_MAX", "10000"))
INFLUX_BATCH_SIZE = int(os.getenv("INFLUX_BATCH_SIZE", "50"))
ROLLING_WINDOW_SIZE = int(os.getenv("ROLLING_WINDOW_SIZE", "10"))

# ---------------------------------------------------------------------
# Setup Logging
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("subscriber")

msg_queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE_MAX)

# Rolling windows for features
_win_pressure = defaultdict(lambda: deque(maxlen=ROLLING_WINDOW_SIZE))
_win_flow = defaultdict(lambda: deque(maxlen=ROLLING_WINDOW_SIZE))

# Prometheus Metrics
messages_received = Counter("mqtt_messages_received_total", "Total MQTT messages received")
processed_messages_sent = Counter("mqtt_processed_sent_total", "Total processed messages republished (Edge mode)")
influx_writes_success = Counter("influxdb_writes_success_total", "Successful InfluxDB writes")
influx_writes_failed = Counter("influxdb_writes_failed_total", "Failed InfluxDB writes")
leaks_detected = Counter("pipeline_leaks_detected_total", "Number of potential leaks detected")
queue_depth = Gauge("subscriber_queue_depth", "Current subscriber queue depth")

start_http_server(8000)

# ---------------------------------------------------------------------
# InfluxDB Helper
# ---------------------------------------------------------------------
client = None
write_api = None

def setup_influx():
    global client, write_api
    if MODE != "CLOUD":
        return
        
    for attempt in range(10):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api = client.write_api(write_options=SYNCHRONOUS)
            log.info("Connected to InfluxDB.")
            return
        except Exception as e:
            log.warning(f"InfluxDB connection failed ({e}), retrying...")
            time.sleep(2)
    log.error("Could not connect to InfluxDB.")

# ---------------------------------------------------------------------
# Logic: Water System Feature Extraction
# ---------------------------------------------------------------------
def process_water_sim_data(data):
    """
    Analyze raw telemetry from the WaterPumpStation.
    Returns:
       dict of features to store/forward
       bool is_alert (Critical event?)
    """
    device_id = data.get("device_id")
    pressure = float(data.get("pressure_psi", 0))
    flow = float(data.get("flow_gpm", 0))
    valve = float(data.get("valve_position", 0))
    
    # 1. Update Windows
    _win_pressure[device_id].append(pressure)
    _win_flow[device_id].append(flow)
    
    avg_pressure = sum(_win_pressure[device_id]) / len(_win_pressure[device_id])
    avg_flow = sum(_win_flow[device_id]) / len(_win_flow[device_id])
    
    # 2. Leak Detection Logic (Edge Intelligence)
    # Symptom: High Flow but Low Pressure (Fluid escaping before sensor)
    # Simplify: If Valve is open, Flow should be proportional to Pressure.
    # If Flow is HIGH but Pressure is suspiciously LOW, that's a leak.
    
    is_leak = False
    is_alert = False
    
    expected_flow = pressure * valve * 0.05
    # If observed flow is significantly higher than physics suggests
    if valve > 10 and flow > (expected_flow + 15.0):
        is_leak = True
        leaks_detected.inc()
        is_alert = True
        
    return {
        "pressure_avg": round(avg_pressure, 2),
        "flow_avg": round(avg_flow, 2),
        "leak_flag": int(is_leak),
    }, is_alert

# ---------------------------------------------------------------------
# Worker Thread
# ---------------------------------------------------------------------
def worker():
    setup_influx()
    mqtt_publisher = None
    
    if MODE == "EDGE":
        mqtt_publisher = mqtt.Client(client_id="edge_processor")
        mqtt_publisher.connect(BROKER, 1883, 60)
        mqtt_publisher.loop_start()

    batch = []
    
    while True:
        try:
            raw_msg = msg_queue.get(timeout=1)
        except queue.Empty:
            continue
            
        try:
            data = json.loads(raw_msg.payload.decode())
            device_id = data.get("device_id")
            site_id = data.get("site_id", "unknown")
            
            # --- Processing ---
            features, is_alert = process_water_sim_data(data)
            
            # Merge raw data with features
            full_record = {**data, **features}
            
            # --- CLOUD MODE: Write everything to Influx ---
            if MODE == "CLOUD":
                p = Point("water_pipeline") \
                    .tag("device_id", device_id) \
                    .tag("site_id", site_id) \
                    .field("pressure_psi", float(data["pressure_psi"])) \
                    .field("flow_gpm", float(data["flow_gpm"])) \
                    .field("valve_position", float(data["valve_position"])) \
                    .field("pump_status", int(data["pump_status"])) \
                    .field("tank_level", float(data["tank_level_pct"])) \
                    .field("pressure_avg", float(features["pressure_avg"])) \
                    .field("leak_flag", int(features["leak_flag"]))
                
                batch.append(p)
                
                if len(batch) >= INFLUX_BATCH_SIZE:
                    try:
                        write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG, record=batch)
                        influx_writes_success.inc(len(batch))
                    except Exception as e:
                        log.error(f"Write failed: {e}")
                        influx_writes_failed.inc()
                    finally:
                        batch.clear()

            # --- EDGE MODE: Forward Processed Data ---
            elif MODE == "EDGE":
                # In Edge mode, we might NOT send everything. 
                # For now, let's send the full enriched record to the 'processed' topic
                # In future steps, we will implement the throttling/feedback logic here.
                
                # Only publish if Alert OR periodic sample (basic compression)
                # For baseline comparison, let's just republish everything for now 
                # but to a DIFFERENT topic to show processing happened.
                payload = json.dumps(full_record)
                mqtt_publisher.publish(TOPIC_PROCESSED, payload)
                processed_messages_sent.inc()

        except Exception as e:
            log.error(f"Error processing message: {e}")
        finally:
            queue_depth.set(msg_queue.qsize())

# ---------------------------------------------------------------------
# MQTT Callbacks
# ---------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    log.info("Connected to Broker. Subscribing to %s", TOPIC)
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    try:
        msg_queue.put_nowait(msg)
        messages_received.inc()
    except queue.Full:
        pass

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    log.info("Starting Subscriber Service in [%s] Mode...", MODE)
    
    # Start Worker
    t = Thread(target=worker, daemon=True)
    t.start()
    
    # Start MQTT Listener
    client = mqtt.Client()
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, 1883, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
