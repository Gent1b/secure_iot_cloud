import os
import json
import time
import logging
import queue
import threading
from collections import defaultdict, deque

import paho.mqtt.client as mqtt
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
# Local Broker (Input from Sensors)
LOCAL_BROKER = os.getenv("LOCAL_BROKER", "localhost")
LOCAL_PORT = int(os.getenv("LOCAL_PORT", 1883))
LOCAL_TOPIC = os.getenv("LOCAL_TOPIC", "iot/devices")

# Central Broker (Output to Cloud + Input for Commands)
CENTRAL_BROKER = os.getenv("CENTRAL_BROKER", "34.185.144.185")
CENTRAL_PORT = int(os.getenv("CENTRAL_PORT", 1883))
CENTRAL_TOPIC_DATA = os.getenv("CENTRAL_TOPIC_DATA", "iot/data")
CENTRAL_TOPIC_CONTROL = os.getenv("CENTRAL_TOPIC_CONTROL", "iot/control")

SITE_ID = os.getenv("SITE_ID", "site_a")

# Tunables
AGGREGATION_WINDOW = int(os.getenv("AGGREGATION_WINDOW", 60)) # seconds
# Modes: NORMAL (1Hz avg), DEBUG (Raw 50Hz), ECONOMY (5min avg)
CURRENT_MODE = "NORMAL" 

# ---------------------------------------------------------------------
# Logging & Metrics
# ---------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format=f"%(asctime)s [{SITE_ID}] [%(levelname)s] %(message)s")
log = logging.getLogger("edge_agent")

# Prometheus
m_ingress = Counter("edge_ingress_messages_total", "Messages received from local sensors")
m_egress = Counter("edge_egress_messages_total", "Messages sent to cloud")
m_leaks = Counter("edge_leaks_detected_total", "Leaks detected locally")
g_mode = Gauge("edge_operating_mode", "Current Mode (0=Economy, 1=Normal, 2=Debug)")

start_http_server(8000)

# ---------------------------------------------------------------------
# State & Buffers
# ---------------------------------------------------------------------
# Buffer for aggregation: device_id -> list of records
data_buffer = defaultdict(list)
buffer_lock = threading.Lock()

# ---------------------------------------------------------------------
# Logic: Water System Analysis
# ---------------------------------------------------------------------
def detect_leak(record):
    """
    Returns True if a leak is suspected based on instantaneous physics.
    Logic: High Flow + Low Pressure + Valve Open
    """
    try:
        pressure = float(record.get("pressure_psi", 0))
        flow = float(record.get("flow_gpm", 0))
        valve = float(record.get("valve_position", 0))
        
        expected_flow = pressure * valve * 0.05
        # If flow is much higher than physics predicts
        if valve > 10 and flow > (expected_flow + 15.0):
            return True
    except:
        pass
    return False

def aggregate_data(device_id, records):
    """
    Compresses a list of records into a single average record.
    Preserves categorical fields (valve_position, pump_status) from last record.
    Preserves MAX leak_flag to ensure safety Critical events are not lost.
    """
    if not records:
        return None
        
    count = len(records)
    avg_pressure = sum(r["pressure_psi"] for r in records) / count
    avg_flow = sum(r["flow_gpm"] for r in records) / count
    avg_level = sum(r["tank_level_pct"] for r in records) / count
    
    # Use the last record for timestamp and categorical fields
    last_record = records[-1]
    
    # SAFETY FIX: PRESERVE LEAK FLAG
    # Check if ANY record in the buffer has a leak_flag (ground truth)
    # This ensures that even if the alert was missed or threshold was edge-case,
    # the cloud gets the signal.
    max_leak_flag = max((r.get("leak_flag", 0) for r in records), default=0)
    
    return {
        "device_id": device_id,
        "site_id": SITE_ID,
        "timestamp": last_record["timestamp"],
        "pressure_psi": round(avg_pressure, 2),
        "flow_gpm": round(avg_flow, 2),
        "valve_position": last_record.get("valve_position", 0),
        "pump_status": last_record.get("pump_status", 0),
        "tank_level_pct": round(avg_level, 2),
        "aggregation_count": count,
        "mode": CURRENT_MODE,
        "leak_flag": max_leak_flag
    }

# ---------------------------------------------------------------------
# MQTT Clients
# ---------------------------------------------------------------------
local_client = mqtt.Client(client_id=f"edge_local_{SITE_ID}")
central_client = mqtt.Client(client_id=f"edge_uplink_{SITE_ID}")

def on_local_message(client, userdata, msg):
    global CURRENT_MODE
    m_ingress.inc()
    
    try:
        payload = json.loads(msg.payload.decode())
        
        # 1. Immediate Leak Check (Safety Critical)
        if detect_leak(payload):
            log.warning(f"LEAK DETECTED on {payload.get('device_id')}! Sending ALERT.")
            m_leaks.inc()
            # Priority Upload
            alert_payload = payload.copy()
            alert_payload["alert_type"] = "LEAK_DETECTED"
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(alert_payload), qos=1)
            
        # 2. Routing based on Mode
        if CURRENT_MODE == "DEBUG":
            # Passthrough Mode: Send everything raw
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(payload))
            m_egress.inc()
        else:
            # Aggregation Mode: Buffer it
            with buffer_lock:
                data_buffer[payload.get("device_id")].append(payload)
                
    except Exception as e:
        log.error(f"Error processing local msg: {e}")

def on_central_message(client, userdata, msg):
    global CURRENT_MODE
    try:
        # Topic: iot/control/{site_id}
        # Payload: {"mode": "DEBUG"}
        cmd = json.loads(msg.payload.decode())
        new_mode = cmd.get("mode")
        
        if new_mode in ["NORMAL", "DEBUG", "ECONOMY"]:
            log.info(f"Received Command: Change Mode {CURRENT_MODE} -> {new_mode}")
            CURRENT_MODE = new_mode
            
            # Update Metric
            val = 1
            if new_mode == "ECONOMY": val = 0
            if new_mode == "DEBUG": val = 2
            g_mode.set(val)
            
    except Exception as e:
        log.error(f"Error processing command: {e}")

# ---------------------------------------------------------------------
# Aggregation Loop
# ---------------------------------------------------------------------
def aggregation_worker():
    while True:
        # Dynamic Sleep based on Mode
        # NORMAL: 60s, ECONOMY: 300s (5 mins), DEBUG: 1s (or skip)
        target_window = AGGREGATION_WINDOW
        if CURRENT_MODE == "ECONOMY":
            target_window = 300
        elif CURRENT_MODE == "DEBUG":
            target_window = 1

        # Sleep in 1s chunks to allow rapid reaction if mode changes
        for _ in range(target_window):
            if CURRENT_MODE == "DEBUG":
                break # Exit sleep immediately if we switch to DEBUG
            time.sleep(1)
        
        if CURRENT_MODE == "DEBUG":
            time.sleep(1)
            continue # No aggregation in debug mode
            
        log.info(f"Running aggregation cycle (Window: {target_window}s)...")
        
        with buffer_lock:
            # Snapshot and clear buffer
            snapshot = data_buffer.copy()
            data_buffer.clear()
            
        for device_id, records in snapshot.items():
            if not records:
                continue
                
            agg_record = aggregate_data(device_id, records)
            if agg_record:
                central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(agg_record))
                m_egress.inc()

# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------
def main():
    log.info(f"Starting Edge Agent for {SITE_ID}")
    log.info(f"Local Broker: {LOCAL_BROKER}")
    log.info(f"Central Broker: {CENTRAL_BROKER}")
    
    # Connect Central (Upstream)
    try:
        central_client.on_message = on_central_message
        central_client.connect(CENTRAL_BROKER, CENTRAL_PORT, 60)
        central_client.subscribe(f"{CENTRAL_TOPIC_CONTROL}/{SITE_ID}")
        central_client.loop_start()
        log.info("Connected to Central Cloud.")
    except Exception as e:
        log.error(f"Failed to connect to Central Cloud: {e}")
        return

    # Connect Local (Downstream)
    try:
        local_client.on_message = on_local_message
        local_client.connect(LOCAL_BROKER, LOCAL_PORT, 60)
        local_client.subscribe(LOCAL_TOPIC)
        local_client.loop_start()
        log.info("Connected to Local Sensors.")
    except Exception as e:
        log.error(f"Failed to connect to Local Broker: {e}")
        return

    # Start Aggregator
    t = threading.Thread(target=aggregation_worker, daemon=True)
    t.start()
    
    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping...")
        local_client.loop_stop()
        central_client.loop_stop()

if __name__ == "__main__":
    main()
