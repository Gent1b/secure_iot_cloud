import paho.mqtt.client as mqtt
import random
import time
import os
import json
import threading
import logging

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    force=True,
)
log = logging.getLogger("publisher")

# ---------- Configuration ----------
BROKER = os.getenv("BROKER", "mqtt_broker")
PORT = 1883
TOPIC = os.getenv("MQTT_TOPIC", "iot/devices")
DEVICES_PER_CONTAINER = int(os.getenv("DEVICES_PER_CONTAINER", 50))
PUBLISH_INTERVAL = float(os.getenv("PUBLISH_INTERVAL", 10))

# ---------- Grouping / topology (no envs) ----------
SITES = ["plant-a", "plant-b", "plant-c"]  # logical CI sites


def classify_device_site(device_id: str) -> str:
    """
    Derive a site_id deterministically from the device index.
    Assumes device_id looks like '<base>_<index>'.
    """
    try:
        idx_raw = int(device_id.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        idx_raw = 1

    idx = max(idx_raw - 1, 0)
    site_id = SITES[idx % len(SITES)]
    return site_id


# ---------- Sensor data ----------
def generate_sensor_data(device_id):
    temperature = round(random.uniform(20.0, 30.0), 2)
    humidity = round(random.uniform(40.0, 60.0), 2)

    # occasional spikes
    if random.random() < 0.1:
        temperature += random.uniform(10.0, 20.0) * random.choice([-1, 1])
        humidity += random.uniform(10.0, 20.0) * random.choice([-1, 1])

    site_id = classify_device_site(device_id)

    return {
        "device_id": device_id,
        "site_id": site_id,
        "temperature": round(temperature, 2),
        "humidity": round(humidity, 2),
    }


# ---------- MQTT helpers ----------
def connect_with_retry(client, device_id):
    """Try to connect until the broker is available."""
    delay = 2
    while True:
        try:
            client.connect(BROKER, PORT, keepalive=300)
            log.info("[%s] Connected to broker %s:%d", device_id, BROKER, PORT)
            return
        except Exception as e:
            log.warning(
                "[%s] Connection failed (%s); retrying in %ds",
                device_id,
                e,
                delay,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30)  # exponential backoff


def on_disconnect(client, userdata, rc):
    """Triggered when MQTT connection is lost."""
    if rc != 0:
        log.warning("[%s] Disconnected (rc=%s). Reconnecting…", userdata["device_id"], rc)
        connect_with_retry(client, userdata["device_id"])


# ---------- Device simulation ----------
def simulate_device(device_id):
    client = mqtt.Client(client_id=f"{device_id}")
    client.user_data_set({"device_id": device_id})
    client.enable_logger()
    client.on_disconnect = on_disconnect
    client.reconnect_delay_set(min_delay=2, max_delay=30)

    # add random startup delay to avoid connection storm
    time.sleep(random.uniform(0, 3))

    # connect initially
    connect_with_retry(client, device_id)
    client.loop_start()

    counter = 0
    while True:
        payload = json.dumps(generate_sensor_data(device_id))
        try:
            result = client.publish(TOPIC, payload, qos=0)
            if result.rc != mqtt.MQTT_ERR_SUCCESS:
                log.warning("[%s] Publish failed (rc=%s)", device_id, result.rc)
            counter += 1
            if counter % 100 == 0:
                log.info("[%s] Published %d messages", device_id, counter)
        except Exception as e:
            log.error("[%s] Publish exception (%s); reconnecting…", device_id, e)
            connect_with_retry(client, device_id)
        time.sleep(PUBLISH_INTERVAL)


# ---------- Main ----------
if __name__ == "__main__":
    base = os.getenv("HOSTNAME", "sim")
    log.info(
        "Starting simulator on %s, creating %d virtual devices",
        base,
        DEVICES_PER_CONTAINER,
    )
    threads = []
    for i in range(DEVICES_PER_CONTAINER):
        device_id = f"{base}_{i+1}"
        t = threading.Thread(target=simulate_device, args=(device_id,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.05)  # small stagger helps prevent startup burst
    for t in threads:
        t.join()
