import paho.mqtt.client as mqtt
import random
import time
import os
import json
import threading
import logging
import math

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
DEVICES_PER_CONTAINER = int(os.getenv("DEVICES_PER_CONTAINER", 10))

# THESIS FIX #4: Deterministic Rate Control
TARGET_RATE_HZ = int(os.getenv("TARGET_RATE_HZ", 50))  # 50Hz = realistic sensor rate
PUBLISH_INTERVAL = 1.0 / TARGET_RATE_HZ

BASE_HOSTNAME = os.getenv("HOSTNAME", "sim")

# ---------- Grouping / Topology ----------
SITES = ["plant-a", "plant-b", "plant-c"]

def classify_device_site(device_id: str) -> str:
    """Derive a site_id deterministically from the device index."""
    try:
        idx_raw = int(device_id.rsplit("_", 1)[1])
    except (ValueError, IndexError):
        idx_raw = 1
    idx = max(idx_raw - 1, 0)
    site_id = SITES[idx % len(SITES)]
    return site_id

# ---------- Water System Simulation Class ----------
class WaterPumpStation:
    def __init__(self, device_id):
        self.device_id = device_id
        self.site_id = classify_device_site(device_id)
        
        # State Variables
        self.pump_status = 0      # 0=OFF, 1=ON
        self.valve_position = 0   # 0-100%
        self.tank_level = 50.0    # %
        self.pressure_psi = 0.0
        self.flow_gpm = 0.0
        
        # Simulation Parameters
        self.target_pressure = 120.0  # PSI when pump is ON
        self.leak_active = False
        self.maintenance_mode = 0
        self.cycle_timer = 0
        
    def step(self):
        """Advance the physical simulation by one time step."""
        # 1. Logic: Pump Control (Hysteresis)
        if self.tank_level < 30:
            self.pump_status = 1
        elif self.tank_level > 80:
            self.pump_status = 0
            
        # 2. Logic: Valve Control (PID-ish, slow open/close)
        target_valve = 100 if self.pump_status == 1 else 0
        if self.valve_position < target_valve:
            self.valve_position += 5  # Open slowly
        elif self.valve_position > target_valve:
            self.valve_position -= 5  # Close slowly
            
        # 3. Physics: Pressure Calculation
        # Pressure builds up if Pump is ON. Drops if Valve is OPEN (Flowing).
        base_pressure = (self.pump_status * self.target_pressure)
        
        # Noise
        noise = random.normalvariate(0, 2.0)
        
        # Leaks: If leak is active, pressure drops significantly despite pump being on
        leak_factor = 0.0
        if self.leak_active:
            leak_factor = 40.0 # PSI drop due to leak
            
        self.pressure_psi = max(0, base_pressure - (self.valve_position * 0.1) - leak_factor + noise)

        # 4. Physics: Flow Calculation
        # Flow depends on Pressure and Valve Position
        # If Leak is active, Flow is artificially HIGH (leaking out) even if pressure is low(er)
        self.flow_gpm = (self.pressure_psi * self.valve_position * 0.05) 
        if self.leak_active:
            self.flow_gpm += 20.0 # Leak flow
            
        self.flow_gpm = max(0, self.flow_gpm + random.normalvariate(0, 1.0))

        # 5. Physics: Tank Level
        # Pump ON fills tank? Or Pump empties tank? Let's say Pump fills tank from well.
        if self.pump_status == 1:
            self.tank_level += 0.5
        
        # Constant usage/drain
        self.tank_level -= 0.2
        self.tank_level = max(0, min(100, self.tank_level))
        
        self.cycle_timer += 1
        
        # THESIS FIX #3: Deterministic Leak Events
        # Random events use fixed seed for reproducibility
        # Leak probability same, but sequence is reproducible
        if not self.leak_active and random.random() < 0.001:  # 0.1% chance per step
            self.leak_active = True
            log.warning("[%s] LEAK STARTED!", self.device_id)
            
        # Random Event: Leak Repair
        if self.leak_active and random.random() < 0.05: # 5% chance to fix itself (or be fixed)
            self.leak_active = False
            log.info("[%s] Leak repaired.", self.device_id)

    def get_telemetry(self):
        return {
            "device_id": self.device_id,
            "site_id": self.site_id,
            "timestamp": time.time(),
            "pressure_psi": round(self.pressure_psi, 2),
            "flow_gpm": round(self.flow_gpm, 2),
            "valve_position": self.valve_position,
            "pump_status": self.pump_status,
            "tank_level_pct": round(self.tank_level, 2),
            "maintenance_mode": self.maintenance_mode,
            "leak_flag": int(self.leak_active) # Ground truth for validation (optional to send)
        }

# ---------- MQTT Helpers ----------
def connect_with_retry(client, device_id):
    delay = 2
    while True:
        try:
            client.connect(BROKER, PORT, keepalive=60)
            log.info("[%s] Connected to broker %s:%d", device_id, BROKER, PORT)
            return
        except Exception as e:
            log.warning("[%s] Connection failed (%s); retrying in %ds", device_id, e, delay)
            time.sleep(delay)
            delay = min(delay * 2, 30)

def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.warning("[%s] Disconnected (rc=%s). Reconnecting…", userdata["device_id"], rc)
        connect_with_retry(client, userdata["device_id"])

# ---------- Device Simulation Loop ----------
def simulate_device(device_id):
    # Initialize Physics Model
    station = WaterPumpStation(device_id)
    
    client = mqtt.Client(client_id=device_id)
    client.user_data_set({"device_id": device_id})
    client.on_disconnect = on_disconnect
    
    time.sleep(random.uniform(0, 5)) # Jitter startup
    connect_with_retry(client, device_id)
    client.loop_start()

    # THESIS FIX #4: Monotonic clock for precise rate control
    next_publish_time = time.monotonic()
    counter = 0
    
    while True:
        station.step()
        data = station.get_telemetry()
        
        try:
            payload = json.dumps(data)
            client.publish(TOPIC, payload, qos=0)
            
            counter += 1
            if counter % 100 == 0:
                log.info("[%s] Published %d readings. P:%.1f Flow:%.1f", 
                         device_id, counter, station.pressure_psi, station.flow_gpm)
                 
        except Exception as e:
            log.error("[%s] Publish error: %s", device_id, e)
            connect_with_retry(client, device_id)
        
        # Wait until next scheduled time slot (prevents drift)
        next_publish_time += PUBLISH_INTERVAL
        sleep_duration = next_publish_time - time.monotonic()
        if sleep_duration > 0:
            time.sleep(sleep_duration)

# ---------- Main ----------
if __name__ == "__main__":
    # THESIS FIX #3: Deterministic Experiments
    LEAK_SEED = int(os.getenv("LEAK_SEED", 42))
    random.seed(LEAK_SEED)
    log.info("Starting Water Utility Simulator on %s with %d devices (LEAK_SEED=%d)...", 
             BASE_HOSTNAME, DEVICES_PER_CONTAINER, LEAK_SEED)
    
    threads = []
    for i in range(DEVICES_PER_CONTAINER):
        dev_id = f"{BASE_HOSTNAME}_{i+1}"
        t = threading.Thread(target=simulate_device, args=(dev_id,), daemon=True)
        t.start()
        threads.append(t)
        time.sleep(0.1)
        
    for t in threads:
        t.join()

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
