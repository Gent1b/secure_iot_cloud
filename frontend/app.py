import streamlit as st
import paho.mqtt.client as mqtt
import json
import time
import threading
import os
import pandas as pd
import queue
import requests
from datetime import datetime

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BROKER = os.getenv("BROKER", "172.31.39.30")
PORT = 1883
TOPIC_CONTROL = "iot/control/#"
TOPIC_DATA = "iot/data/#"
PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://172.31.42.61:9090")
SUBSCRIBER_METRICS_URL = "http://subscriber:8000/metrics"  # Internal docker network

# ---------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------
if "logs" not in st.session_state:
    st.session_state.logs = []
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None
if "mqtt_connected" not in st.session_state:
    st.session_state.mqtt_connected = False
if "last_refresh" not in st.session_state:
    st.session_state.last_refresh = time.time()

# Queue for thread-safe communication
msg_queue = queue.Queue()

# ---------------------------------------------------------------------
# MQTT Logic
# ---------------------------------------------------------------------
def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        topic = msg.topic
        timestamp = time.strftime("%H:%M:%S")
        
        entry = {
            "time": timestamp,
            "topic": topic,
            "payload": payload
        }
        msg_queue.put(entry)
    except Exception as e:
        print(f"Error: {e}")mqtt.CallbackAPIVersion.VERSION1, client_id="frontend_dashboard")
        try:
            client.connect(BROKER, PORT, 60)
            client.subscribe(TOPIC_CONTROL)
            client.subscribe(TOPIC_DATA)
            client.on_message = on_message
            client.loop_start()
            st.session_state.mqtt_client = client
            st.session_state.mqtt_connected = True
        except Exception as e:
            st.error(f"Could not connect to MQTT Broker at {BROKER}: {e}")
            st.session_state.mqtt_connected = False

def get_cloud_cpu():
    """Query Prometheus for Cloud Node CPU usage"""
    try:
        query = '100 - (avg(rate(node_cpu_seconds_total{job="cloud_node_exporter",mode="idle"}[1m])) * 100)'
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query}, timeout=2)
        if response.status_code == 200:
            data = response.json()
            if data['data']['result']:
                return float(data['data']['result'][0]['value'][1])
    except:
        pass
    return None

def get_subscriber_metrics():
    """Get metrics from Subscriber service"""
    try:
        response = requests.get(SUBSCRIBER_METRICS_URL, timeout=2)
        if response.status_code == 200:
            lines = response.text.split('\n')
            metrics = {}
            for line in lines:
                if line.startswith('mqtt_messages_arrived_total'):
                    metrics['arrived'] = float(line.split()[1])
                elif line.startswith('influxdb_writes_success_total'):
                    metrics['writes'] = float(line.split()[1])
                elif line.startswith('pipeline_leaks_detected_total'):
                    metrics['leaks'] = float(line.split()[1])
            return metrics
    except:
        pass
    return {}
            client.loop_start()
            st.session_state.mqtt_client = client
        except Exception as e:
            st.error(f"Could not connect to MQTT Broker at {BROKER}: {e}")

def publish_leak(site_id):
    if st.session_state.mqtt_client:
        payload = {
            "device_id": "simulated-injector",
            "site_id": site_id,
            "timestamp": time.time(),
            "pressure_psi": 10.0,
            "flow_gpm": 150.0,
            "valve_position": 100,
            "pump_status": 1,
            "tank_level_pct": 40.0,
            "leak_flag": 1, # Explicit leak
            "mode": "DEMO"
        }
        topic = f"iot/data/{site_id}"
        st.session_state.mqtt_client.publish(topic, json.dumps(payload))
        st.success(f"Injecting LEAK event to {topic}")

def publish_normal(site_id):
    if st.session_state.mqtt_client:
        payload = {
            "device_id": "simulated-injector",
            "site_id": site_id,
            "timestamp": time.time(),
            "pressure_psi": 50.0,
            "flow_gpm": 250.0,
            "valve_position": 100,
            "pump_status": 1,
            "tank_level_pct": 60.0,
            "leak_flag": 0,
            "mode": "DEMO"
        }
        topic = f"iot/data/{site_id}"
        st.session_state.mqtt_client.publish(topic, json.dumps(payload))
        st.info(f"Injecting NORMAL event to {topic}")

# Connection Status
if st.session_state.mqtt_connected:
    st.success(f"✅ Connected to MQTT Broker ({BROKER})")
else:
    st.error(f"❌ Not connected to MQTT Broker ({BROKER})")

# Process incoming messages
while not msg_queue.empty():
    st.session_state.logs.insert(0, msg_queue.get())
    # Keep log size manageable
    if len(st.session_state.logs) > 50:
        st.session_state.logs.pop()

# Cloud Metrics
st.subheader("Cloud Infrastructure Status")
col_cpu, col_msgs, col_writes, col_leaks = st.columns(4)

cloud_cpu = get_cloud_cpu()
subscriber_metrics = get_subscriber_metrics()

with col_cpu:
    if cloud_cpu is not None:
        st.metric("Cloud CPU Usage", f"{cloud_cpu:.1f}%", delta="-Good" if cloud_cpu < 50 else "High")
    else:
        st.metric("Cloud CPU Usage", "N/A")

with col_msgs:
    if 'arrived' in subscriber_metrics:
        st.metric("Messages Received", int(subscriber_metrics['arrived']))
    else:
        st.metric("Messages Received", "N/A")

with col_writes:
    if 'writes' in subscriber_metrics:
        st.metric("InfluxDB Writes", int(subscriber_metrics['writes']))
    else:
        st.metric("InfluxDB Writes", "N/A")

with col_leaks:
    if 'leaks' in subscriber_metrics:
        st.metric("Leaks Detected", int(subscriber_metrics['leaks']), delta="⚠️" if subscriber_metrics['leaks'] > 0 else None)
    else:
        st.metric("Leaks Detected", "N/A")

# Display System State (Inferred from logs)
st.divider()
st.subheader("Edge Agent Operating Modes
with st.sidebar:
    st.header("Simulation Controls")
    st.markdown("Trigger events to test the feedback loop.")
    
    st.subheader("Plant A")
    col1, col2 = st.columns(2)
    if col1.button("🔥 LEAK (A)"):
        publish_leak("plant-a")
    if col2.button("✅ Normal (A)"):
        publish_normal("plant-a")

    st.subheader("Plant B")
    col3, col4 = st.columns(2)
    if col3.button("🔥 LEAK (B)"):
        publish_leak("plant-b")
    if col4.button("✅ Normal (B)"):
        publish_normal("plant-b")

   divider()
st.subheader("Feedback Loop Activity Log")
st.markdown(f"*Listening to `iot/control/#` and `iot/data/#`... (Last {len(st.session_state.logs)} events)*")

# Convert logs to DataFrame for display
if st.session_state.logs:
    # Only show last 20 for readability
    recent_logs = st.session_state.logs[:20]
    df = pd.DataFrame(recent_logs)
    
    # Format payload as string for display
    df['payload_str'] = df['payload'].apply(lambda x: json.dumps(x, indent=2))
    df_display = df[['time', 'topic', 'payload_str']]
    
    st.dataframe(df_display, use_container_width=True, height=400)
else:
    st.info("No messages received yet. Waiting for system activity...")

# Auto-refresh every 5 seconds
if time.time() - st.session_state.last_refresh > 5:
    st.session_state.last_refresh = time.time()
            st.session_state.logs.pop()

# Display System State (Inferred from logs)
st.subheader("System State (Real-time)")
col_a, col_b, col_c = st.columns(3)

# Helper to find last mode for a site
def get_last_mode(site):
    for log in st.session_state.logs:
        if f"iot/control/{site}" in log["topic"]:
            return log["payload"].get("mode", "UNKNOWN")
    return "WAITING..."

with col_a:
    mode_a = get_last_mode("plant-a")
    st.metric("Plant A Mode", mode_a, delta="Active" if mode_a=="DEBUG" else None)

with col_b:
    mode_b = get_last_mode("plant-b")
    st.metric("Plant B Mode", mode_b, delta="Active" if mode_b=="DEBUG" else None)

with col_c:
    mode_c = get_last_mode("plant-c")
    st.metric("Plant C Mode", mode_c, delta="Active" if mode_c=="DEBUG" else None)

# Live Log Feed
st.subheader("Feedback Loop Activity Log")
st.markdown("*Listening to `iot/control/#` and `iot/data/#`...*")

# Convert logs to DataFrame for display
if st.session_state.logs:
    df = pd.DataFrame(st.session_state.logs)
    st.dataframe(df, use_container_width=True)
else:
    st.info("No messages received yet. Waiting for system activity...")

# Auto-refresh hack (Streamlit doesn't auto-refresh by default)
time.sleep(1)
st.rerun()
