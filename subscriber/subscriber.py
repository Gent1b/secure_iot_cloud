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
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ---------------------------------------------------------------------
# Environment configuration
# ---------------------------------------------------------------------
BROKER = os.getenv("BROKER", "mqtt_broker")
TOPIC = os.getenv("MQTT_TOPIC", "iot/devices")

# InfluxDB Configuration
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "local_token_123")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "secure_iot")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")

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

# ---------------------------------------------------------------------
# Prometheus Metrics (The Observability Pipeline)
# ---------------------------------------------------------------------

# 1. Pipeline Stages
m_arrived = Counter("mqtt_messages_arrived_total", "Total MQTT messages arrived at network callback (Absolute Truth)")
m_buffered = Counter("mqtt_messages_buffered_total", "Total MQTT messages successfully enqueued")
m_dropped = Counter("mqtt_messages_dropped_total", "Total MQTT messages dropped due to full queue (System Stress)")
m_processed = Counter("app_messages_processed_total", "Total messages processed by worker thread logic")

# 2. Thesis Evaluation Metrics (Academic)
bandwidth_usage = Counter("iot_bandwidth_bytes_total", "Total telemetry payload bytes received (Cost)")
network_latency = Histogram("iot_network_latency_seconds", "Network transit time: MQTT receipt - sensor timestamp")
processing_latency = Histogram("iot_processing_latency_seconds", "Processing time: InfluxDB write start - MQTT receipt")
end_to_end_latency = Histogram("iot_end_to_end_latency_seconds", "Total latency: InfluxDB write - sensor timestamp")

# 3. Egress & Business Metrics
influx_writes_success = Counter("influxdb_writes_success_total", "Successful InfluxDB points written")
influx_writes_failed = Counter("influxdb_writes_failed_total", "Failed InfluxDB points written")
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
def process_water_sim_data(data, mqtt_receipt_time):
    """
    Analyze raw telemetry from the WaterPumpStation.
    Returns:
       dict of features to store/forward
       bool is_alert (Critical event?)
    """
    # Thesis Metrics: Latency breakdown
    try:
        if "timestamp" in data:
            sensor_time = float(data["timestamp"])
            current_time = time.time()
            
            # Network latency: Time data spent in transit
            net_latency = mqtt_receipt_time - sensor_time
            if net_latency > 0:
                network_latency.observe(net_latency)
            
            # End-to-end latency: Total time from sensor to now
            e2e_latency = current_time - sensor_time
            if e2e_latency > 0:
                end_to_end_latency.observe(e2e_latency)
    except Exception:
        pass

    device_id = data.get("device_id")
    pressure = float(data.get("pressure_psi", 0))
    flow = float(data.get("flow_gpm", 0))
    valve = float(data.get("valve_position", 0))
    
    # 1. Update Windows
    _win_pressure[device_id].append(pressure)
    _win_flow[device_id].append(flow)
    
    avg_pressure = sum(_win_pressure[device_id]) / len(_win_pressure[device_id])
    avg_flow = sum(_win_flow[device_id]) / len(_win_flow[device_id])
    
    # 2. Leak Detection Logic (Edge Intelligence / Global Validation)
    # Symptom: High Flow but Low Pressure (Fluid escaping before sensor)
    # Simplify: If Valve is open, Flow should be proportional to Pressure.
    # If Flow is HIGH but Pressure is suspiciously LOW, that's a leak.
    
    # Cloud Validation Logic (Physics Based)
    is_cloud_detected_leak = False
    
    expected_flow = pressure * valve * 0.05
    if valve > 10 and flow > (expected_flow + 15.0):
        is_cloud_detected_leak = True
        leaks_detected.inc() # Increment Prometheus Metric for "Cloud Detections"
        is_alert = True # Treat as alert for immediate storage tagging

    # GROUND TRUTH RECONCILIATION
    # The record might come with a 'leak_flag' from the Edge (Ground Truth or Aggregated Truth).
    # If available, we trust it for the 'leak_flag' field in InfluxDB, 
    # to ensure the feedback loop triggers on the Sensor's truth, not just our estimation.
    # If not present (legacy or stripped), we fall back to cloud detection.
    
    final_leak_flag = int(data.get("leak_flag", is_cloud_detected_leak))

    return {
        "pressure_avg": round(avg_pressure, 2),
        "flow_avg": round(avg_flow, 2),
        "leak_flag": final_leak_flag,
    }, is_alert

# ---------------------------------------------------------------------
# Worker Thread
# ---------------------------------------------------------------------
def worker():
    setup_influx()
    batch = []
    
    while True:
        try:
            raw_msg = msg_queue.get(timeout=1)
        except queue.Empty:
            continue
        
        # Capture MQTT receipt time for latency calculation
        mqtt_receipt_time = time.time()
            
        # Pipeline: Picked up for processing
        m_processed.inc()
        
        try:
            payload_str = raw_msg.payload.decode()
            data = json.loads(payload_str)
            device_id = data.get("device_id")
            site_id = data.get("site_id", "unknown")
            
            # --- Processing ---
            features, is_alert = process_water_sim_data(data, mqtt_receipt_time)
            
            # --- Write to InfluxDB ---
            influx_write_start = time.time()
            
            p = Point("water_pipeline") \
                .tag("device_id", device_id) \
                .tag("site_id", site_id)
            
            # Capture Edge Alerts if present
            if "alert_type" in data:
                p.tag("alert_type", data["alert_type"])

            p.field("pressure_psi", float(data["pressure_psi"])) \
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
                    
                    # Processing latency: Time from MQTT receipt to InfluxDB write
                    proc_latency = time.time() - influx_write_start
                    processing_latency.observe(proc_latency)
                except Exception as e:
                    log.error(f"Write failed: {e}")
                    influx_writes_failed.inc(len(batch))
                finally:
                    batch.clear()

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
    # Pipeline: Network Ingress
    m_arrived.inc()
    
    # Thesis Metric: Bandwidth Cost
    # We count raw payload bytes
    bandwidth_usage.inc(len(msg.payload))

    try:
        msg_queue.put_nowait(msg)
        # Pipeline: Buffered
        m_buffered.inc()
    except queue.Full:
        # Pipeline: Drop
        m_dropped.inc()

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    log.info("Starting Cloud Subscriber Service...")
    
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
