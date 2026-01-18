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
RUN_ID = os.getenv("RUN_ID", "run_unknown")
SCENARIO = os.getenv("SCENARIO", "unknown")

# Tunables
AGGREGATION_WINDOW = int(os.getenv("AGGREGATION_WINDOW", 1)) # seconds (NORMAL)
ECONOMY_WINDOW = int(os.getenv("ECONOMY_WINDOW", 300)) # seconds (ECONOMY)
# Modes: NORMAL (1Hz avg), DEBUG (Raw 50Hz), ECONOMY (5min avg)
CURRENT_MODE = "NORMAL" 

MODE_CODE = {
    "NORMAL": 0,
    "DEBUG": 1,
    "ECONOMY": 2,
}

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

# THESIS FIX #7: Mode Transition Traceability
m_mode_transitions = Counter("edge_mode_transitions_total", "Mode transitions", ["from_mode", "to_mode"])

start_http_server(8000)

# ---------------------------------------------------------------------
# State & Buffers
# ---------------------------------------------------------------------
# Buffer for aggregation: device_id -> running stats
data_buffer = defaultdict(lambda: {
    "count": 0,
    "sum_pressure": 0.0,
    "sum_flow": 0.0,
    "sum_level": 0.0,
    "max_leak_truth": 0,
    "max_leak_detected": 0,
    "last_record": None,
})
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

def aggregate_data(device_id, stats):
    """
    Compresses running stats into a single average record.
    Preserves categorical fields (valve_position, pump_status) from last record.
    Preserves MAX leak_truth and leak_detected.
    """
    if not stats or stats["count"] == 0:
        return None

    count = stats["count"]
    avg_pressure = stats["sum_pressure"] / count
    avg_flow = stats["sum_flow"] / count
    avg_level = stats["sum_level"] / count

    last_record = stats["last_record"] or {}

    return {
        "device_id": device_id,
        "site_id": SITE_ID,
        "run_id": last_record.get("run_id", RUN_ID),
        "scenario": last_record.get("scenario", SCENARIO),
        "sample_kind": "agg",
        "timestamp": last_record.get("timestamp", time.time()),
        "pressure_psi": round(avg_pressure, 2),
        "flow_gpm": round(avg_flow, 2),
        "valve_position": last_record.get("valve_position", 0),
        "pump_status": last_record.get("pump_status", 0),
        "tank_level_pct": round(avg_level, 2),
        "aggregation_count": count,
        "mode": CURRENT_MODE,
        "mode_code": MODE_CODE.get(CURRENT_MODE, 0),
        "leak_truth": int(stats["max_leak_truth"]),
        "leak_detected": int(stats["max_leak_detected"]),
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
        payload["run_id"] = payload.get("run_id", RUN_ID)
        payload["scenario"] = payload.get("scenario", SCENARIO)
        payload["sample_kind"] = payload.get("sample_kind", "raw")
        payload["leak_truth"] = int(payload.get("leak_truth", 0))
        
        # 1. Immediate Leak Check (Safety Critical)
        leak_detected = 1 if detect_leak(payload) else 0
        payload["leak_detected"] = leak_detected

        if leak_detected == 1:
            log.warning(f"LEAK DETECTED on {payload.get('device_id')}! Sending ALERT.")
            m_leaks.inc()
            # Priority Upload
            alert_payload = payload.copy()
            alert_payload["alert_type"] = "LEAK_DETECTED"
            alert_payload["sample_kind"] = "status"
            alert_payload["mode_code"] = MODE_CODE.get(CURRENT_MODE, 0)
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(alert_payload), qos=1)
            
        # 2. Routing based on Mode
        if CURRENT_MODE == "DEBUG":
            # Passthrough Mode: Send everything raw
            payload["mode"] = CURRENT_MODE
            payload["mode_code"] = MODE_CODE.get(CURRENT_MODE, 0)
            central_client.publish(f"{CENTRAL_TOPIC_DATA}/{SITE_ID}", json.dumps(payload))
            m_egress.inc()
        else:
            # Aggregation Mode: Streaming stats (memory-safe)
            with buffer_lock:
                device_id = payload.get("device_id")
                stats = data_buffer[device_id]
                stats["count"] += 1
                stats["sum_pressure"] += float(payload.get("pressure_psi", 0))
                stats["sum_flow"] += float(payload.get("flow_gpm", 0))
                stats["sum_level"] += float(payload.get("tank_level_pct", 0))
                stats["max_leak_truth"] = max(stats["max_leak_truth"], int(payload.get("leak_truth", 0)))
                stats["max_leak_detected"] = max(stats["max_leak_detected"], int(payload.get("leak_detected", 0)))
                stats["last_record"] = payload
                
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
            # THESIS FIX #7: Trace mode transitions
            if new_mode != CURRENT_MODE:
                m_mode_transitions.labels(from_mode=CURRENT_MODE, to_mode=new_mode).inc()
                log.info(f"MODE TRANSITION: {CURRENT_MODE} -> {new_mode} (Reason: Controller Command)")
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
        # NORMAL: 1s, ECONOMY: 300s (5 mins), DEBUG: 1s (or skip)
        target_window = AGGREGATION_WINDOW
        if CURRENT_MODE == "ECONOMY":
            target_window = ECONOMY_WINDOW
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
            
        for device_id, stats in snapshot.items():
            agg_record = aggregate_data(device_id, stats)
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
