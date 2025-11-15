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
INFLUXDB_URL = os.getenv("INFLUXDB_URL", "http://influxdb:8086")
INFLUXDB_TOKEN = os.getenv("INFLUXDB_TOKEN", "my-token")
INFLUXDB_ORG = os.getenv("INFLUXDB_ORG", "my-org")
INFLUXDB_BUCKET = os.getenv("INFLUXDB_BUCKET", "iot_data")

# Optional performance/config toggles (all ON by default)
VALIDATE_BASIC = os.getenv("VALIDATE_BASIC", "1") == "1"
DERIVE_FEATURES = os.getenv("DERIVE_FEATURES", "1") == "1"
ROLLING_WINDOW_SEC = int(os.getenv("ROLLING_WINDOW_SEC", "60"))
CHECK_THRESHOLDS = os.getenv("CHECK_THRESHOLDS", "1") == "1"

# Tunable parameters
SUBSCRIBER_QUEUE_MAX = int(os.getenv("SUBSCRIBER_QUEUE_MAX", "10000"))
INFLUX_BATCH_SIZE = int(os.getenv("INFLUX_BATCH_SIZE", "100"))

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

msg_queue = queue.Queue(maxsize=SUBSCRIBER_QUEUE_MAX)
_win_temp = defaultdict(lambda: deque(maxlen=max(1, ROLLING_WINDOW_SEC)))
_win_hum = defaultdict(lambda: deque(maxlen=max(1, ROLLING_WINDOW_SEC)))

# Thresholds for alerts
TEMP_MIN, TEMP_MAX = -10.0, 40.0
HUM_MIN, HUM_MAX = 20.0, 90.0

# ---------------------------------------------------------------------
# Prometheus metrics
# ---------------------------------------------------------------------
messages_received = Counter("mqtt_messages_received_total", "Total MQTT messages received")
influx_writes_success = Counter("influxdb_writes_success_total", "Successful InfluxDB writes")
influx_writes_failed = Counter("influxdb_writes_failed_total", "Failed InfluxDB writes")
queue_depth = Gauge("subscriber_queue_depth", "Current subscriber queue depth")
alerts_triggered = Counter("threshold_alerts_total", "Number of threshold breaches detected")

start_http_server(8000)

# ---------------------------------------------------------------------
# InfluxDB client (with retry loop for startup timing)
# ---------------------------------------------------------------------
def connect_influx(retries=10, delay=5):
    for attempt in range(retries):
        try:
            client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)
            write_api = client.write_api(write_options=SYNCHRONOUS)
            logging.info("Connected to InfluxDB.")
            return client, write_api
        except Exception as e:
            logging.warning(f"InfluxDB connection failed ({e}), retrying {attempt+1}/{retries}...")
            time.sleep(delay)
    raise RuntimeError("Could not connect to InfluxDB after retries")

client, write_api = connect_influx()

# ---------------------------------------------------------------------
# Validation helper
# ---------------------------------------------------------------------
def validate_data(data):
    if not isinstance(data, dict):
        raise ValueError("payload not a dict")
    for key in ("device_id", "temperature", "humidity"):
        if key not in data:
            raise ValueError(f"missing key {key}")
    t = float(data["temperature"])
    h = float(data["humidity"])
    if not (-50 <= t <= 100 and 0 <= h <= 100):
        raise ValueError("out-of-range value")
    return data["device_id"], t, h

# ---------------------------------------------------------------------
# Feature computation helper
# ---------------------------------------------------------------------
def compute_features(device_id, t, h):
    dew = None
    t_avg = h_avg = None
    alert = False

    if DERIVE_FEATURES:
        dew = t - (100.0 - h) / 5.0

    if ROLLING_WINDOW_SEC > 0:
        _win_temp[device_id].append(t)
        _win_hum[device_id].append(h)
        t_avg = sum(_win_temp[device_id]) / len(_win_temp[device_id])
        h_avg = sum(_win_hum[device_id]) / len(_win_hum[device_id])

    if CHECK_THRESHOLDS:
        if t < TEMP_MIN or t > TEMP_MAX or h < HUM_MIN or h > HUM_MAX:
            alert = True
            alerts_triggered.inc()

    return dew, t_avg, h_avg, alert

# ---------------------------------------------------------------------
# MQTT callbacks
# ---------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected to MQTT broker.")
        client.subscribe(TOPIC)
    else:
        logging.error(f"Failed to connect to MQTT broker, code {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        if VALIDATE_BASIC:
            device_id, t, h = validate_data(data)
            data["device_id"], data["temperature"], data["humidity"] = device_id, t, h
        msg_queue.put_nowait(data)
        messages_received.inc()
    except queue.Full:
        logging.warning("Subscriber queue full, message dropped.")
    except Exception as e:
        logging.error(f"Error parsing MQTT message: {e}")

# ---------------------------------------------------------------------
# Writer thread
# ---------------------------------------------------------------------
def influx_writer():
    batch = []
    while True:
        try:
            data = msg_queue.get(timeout=1)
        except queue.Empty:
            continue

        try:
            device_id = data.get("device_id")
            t = float(data.get("temperature"))
            h = float(data.get("humidity"))

            dew, t_avg, h_avg, alert = compute_features(device_id, t, h)

            point = Point("sensor_data") \
                .tag("device_id", device_id) \
                .field("temperature", t) \
                .field("humidity", h)

            if dew is not None:
                point = point.field("dew_point", round(dew, 2))
            if t_avg is not None:
                point = point.field("temp_avg_win", round(t_avg, 2))
            if h_avg is not None:
                point = point.field("hum_avg_win", round(h_avg, 2))
            if alert:
                point = point.field("alert_flag", 1)

            batch.append(point)

            if len(batch) >= INFLUX_BATCH_SIZE:
                write_api.write(bucket=INFLUXDB_BUCKET, org=INFLUXDB_ORG,
                                record=batch, write_precision=WritePrecision.S)
                influx_writes_success.inc(len(batch))
                batch.clear()

        except Exception as e:
            influx_writes_failed.inc()
            logging.error(f"InfluxDB write error: {e}")

        finally:
            queue_depth.set(msg_queue.qsize())

# ---------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------
def main():
    mqtt_client = mqtt.Client()
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message
    mqtt_client.connect(BROKER, 1883, 60)

    writer_thread = Thread(target=influx_writer, daemon=True)
    writer_thread.start()

    logging.info("Subscriber service running with validation, features, and thresholds enabled.")
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()
