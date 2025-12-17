import streamlit as st
import paho.mqtt.client as mqtt
import json
import time
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
SUBSCRIBER_METRICS_URL = "http://subscriber:8000/metrics"

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
        print(f"MQTT Error: {e}")

def start_mqtt():
    if st.session_state.mqtt_client is None:
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION1, client_id="frontend_dashboard")
            client.on_message = on_message
            client.connect(BROKER, PORT, 60)
            client.subscribe(TOPIC_CONTROL)
            client.subscribe(TOPIC_DATA)
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
                elif line.startswith('iot_bandwidth_bytes_total'):
                    metrics['bandwidth'] = float(line.split()[1])
            return metrics
    except:
        pass
    return {}

def publish_leak(site_id):
    if st.session_state.mqtt_client and st.session_state.mqtt_connected:
        payload = {
            "device_id": "demo-injector",
            "site_id": site_id,
            "timestamp": time.time(),
            "pressure_psi": 10.0,
            "flow_gpm": 150.0,
            "valve_position": 100,
            "pump_status": 1,
            "tank_level_pct": 40.0,
            "leak_flag": 1,
            "mode": "DEMO"
        }
        topic = f"iot/data/{site_id}"
        st.session_state.mqtt_client.publish(topic, json.dumps(payload))
        return True
    return False

def publish_normal(site_id):
    if st.session_state.mqtt_client and st.session_state.mqtt_connected:
        payload = {
            "device_id": "demo-injector",
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
        return True
    return False

def get_last_mode(site):
    """Find the most recent mode command for a site"""
    for log in st.session_state.logs:
        if f"iot/control/{site}" in log["topic"]:
            return log["payload"].get("mode", "UNKNOWN")
    return "WAITING..."

# ---------------------------------------------------------------------
# UI Layout
# ---------------------------------------------------------------------
st.set_page_config(page_title="IoT Thesis Controller", layout="wide", page_icon="🚰")

st.title("🚰 Secure IoT System with Edge Computing")
st.markdown("### Real-Time Feedback Loop Controller - Master's Thesis Demonstration")

# Sidebar for Controls
with st.sidebar:
    st.header("🎮 Simulation Controls")
    st.markdown("**Trigger events to demonstrate the feedback loop**")
    
    st.divider()
    st.subheader("Plant A 🏭")
    col1, col2 = st.columns(2)
    if col1.button("🔥 LEAK", key="leak_a", use_container_width=True):
        if publish_leak("plant-a"):
            st.success("✅ Leak injected!")
        else:
            st.error("❌ Not connected")
    
    if col2.button("✅ Normal", key="normal_a", use_container_width=True):
        if publish_normal("plant-a"):
            st.success("✅ Normal sent!")

    st.subheader("Plant B 🏭")
    col3, col4 = st.columns(2)
    if col3.button("🔥 LEAK", key="leak_b", use_container_width=True):
        if publish_leak("plant-b"):
            st.success("✅ Leak injected!")
        else:
            st.error("❌ Not connected")
    
    if col4.button("✅ Normal", key="normal_b", use_container_width=True):
        if publish_normal("plant-b"):
            st.success("✅ Normal sent!")

    st.subheader("Plant C 🏭")
    col5, col6 = st.columns(2)
    if col5.button("🔥 LEAK", key="leak_c", use_container_width=True):
        if publish_leak("plant-c"):
            st.success("✅ Leak injected!")
        else:
            st.error("❌ Not connected")
    
    if col6.button("✅ Normal", key="normal_c", use_container_width=True):
        if publish_normal("plant-c"):
            st.success("✅ Normal sent!")

    st.divider()
    if st.button("🔄 Reconnect MQTT", use_container_width=True):
        st.session_state.mqtt_client = None
        st.session_state.mqtt_connected = False
        st.rerun()
    
    st.divider()
    st.markdown("**Legend:**")
    st.markdown("- 🔥 **LEAK**: Inject anomaly (high flow, low pressure)")
    st.markdown("- ✅ **Normal**: Send regular telemetry")

# Main Dashboard
start_mqtt()

# Connection Status Banner
if st.session_state.mqtt_connected:
    st.success(f"✅ Connected to MQTT Broker `{BROKER}` | Subscribed to `{TOPIC_CONTROL}` and `{TOPIC_DATA}`")
else:
    st.error(f"❌ Disconnected from MQTT Broker `{BROKER}` - Click 'Reconnect MQTT' in sidebar")

# Process incoming messages from queue
while not msg_queue.empty():
    st.session_state.logs.insert(0, msg_queue.get())
    if len(st.session_state.logs) > 100:
        st.session_state.logs.pop()

# === CLOUD INFRASTRUCTURE STATUS ===
st.subheader("☁️ Cloud Infrastructure Metrics")
col_cpu, col_msgs, col_writes, col_leaks, col_bw = st.columns(5)

cloud_cpu = get_cloud_cpu()
subscriber_metrics = get_subscriber_metrics()

with col_cpu:
    if cloud_cpu is not None:
        delta_label = "🟢 Normal" if cloud_cpu < 50 else "🔴 High"
        st.metric("Cloud CPU", f"{cloud_cpu:.1f}%", delta=delta_label)
    else:
        st.metric("Cloud CPU", "N/A")

with col_msgs:
    if 'arrived' in subscriber_metrics:
        st.metric("Messages Received", f"{int(subscriber_metrics['arrived']):,}")
    else:
        st.metric("Messages Received", "0")

with col_writes:
    if 'writes' in subscriber_metrics:
        st.metric("InfluxDB Writes", f"{int(subscriber_metrics['writes']):,}")
    else:
        st.metric("InfluxDB Writes", "0")

with col_leaks:
    if 'leaks' in subscriber_metrics:
        leak_count = int(subscriber_metrics['leaks'])
        st.metric("Leaks Detected", leak_count, delta="⚠️ Alert" if leak_count > 0 else "🟢 Safe")
    else:
        st.metric("Leaks Detected", "0")

with col_bw:
    if 'bandwidth' in subscriber_metrics:
        bw_mb = subscriber_metrics['bandwidth'] / (1024 * 1024)
        st.metric("Bandwidth Used", f"{bw_mb:.2f} MB")
    else:
        st.metric("Bandwidth Used", "0 MB")

# === EDGE AGENT OPERATING MODES ===
st.divider()
st.subheader("⚙️ Edge Agent Operating Modes (Feedback Loop Status)")

col_a, col_b, col_c = st.columns(3)

with col_a:
    mode_a = get_last_mode("plant-a")
    if mode_a == "DEBUG":
        st.success(f"**Plant A**: {mode_a} (High-frequency)")
    elif mode_a == "ECONOMY":
        st.warning(f"**Plant A**: {mode_a} (Throttled)")
    elif mode_a == "NORMAL":
        st.info(f"**Plant A**: {mode_a} (Standard)")
    else:
        st.metric("Plant A", mode_a)

with col_b:
    mode_b = get_last_mode("plant-b")
    if mode_b == "DEBUG":
        st.success(f"**Plant B**: {mode_b} (High-frequency)")
    elif mode_b == "ECONOMY":
        st.warning(f"**Plant B**: {mode_b} (Throttled)")
    elif mode_b == "NORMAL":
        st.info(f"**Plant B**: {mode_b} (Standard)")
    else:
        st.metric("Plant B", mode_b)

with col_c:
    mode_c = get_last_mode("plant-c")
    if mode_c == "DEBUG":
        st.success(f"**Plant C**: {mode_c} (High-frequency)")
    elif mode_c == "ECONOMY":
        st.warning(f"**Plant C**: {mode_c} (Throttled)")
    elif mode_c == "NORMAL":
        st.info(f"**Plant C**: {mode_c} (Standard)")
    else:
        st.metric("Plant C", mode_c)

# === LIVE ACTIVITY LOG ===
st.divider()
st.subheader("📡 Live Feedback Loop Activity")
st.markdown(f"*Real-time MQTT events • Showing last {min(len(st.session_state.logs), 20)} of {len(st.session_state.logs)} total events*")

if st.session_state.logs:
    # Show last 20 events
    recent_logs = st.session_state.logs[:20]
    
    # Create a cleaner display
    display_data = []
    for log in recent_logs:
        payload_summary = ""
        if "mode" in log["payload"]:
            payload_summary = f"Mode: {log['payload']['mode']}"
        elif "leak_flag" in log["payload"]:
            leak = "🔥 LEAK" if log["payload"]["leak_flag"] == 1 else "✅ Normal"
            payload_summary = f"{leak} | P: {log['payload'].get('pressure_psi', 'N/A')} psi | F: {log['payload'].get('flow_gpm', 'N/A')} gpm"
        else:
            payload_summary = str(log["payload"])[:50] + "..."
        
        display_data.append({
            "Time": log["time"],
            "Topic": log["topic"],
            "Message": payload_summary
        })
    
    df = pd.DataFrame(display_data)
    st.dataframe(df, use_container_width=True, height=400, hide_index=True)
else:
    st.info("⏳ Waiting for system activity... Make sure the dynamic scenario is running.")

# === SYSTEM ARCHITECTURE INFO ===
with st.expander("ℹ️ System Architecture Overview"):
    st.markdown("""
    **3-Layer Hierarchical IoT Architecture:**
    - **Edge Layer** (Devices VM): 10 water sensors per plant + Edge agents for local processing
    - **Transport Layer** (MQTT VM): Central message broker (172.31.39.30)
    - **Cloud Layer** (Cloud VM): Subscriber, InfluxDB, Controller (Feedback loop)
    
    **Feedback Loop Logic:**
    1. Controller monitors cloud CPU + InfluxDB for leaks every 10 seconds
    2. If leak detected → Affected plant switches to DEBUG mode (50Hz data)
    3. If cloud CPU > 80% → Non-critical plants throttle to ECONOMY mode (5min aggregation)
    4. Otherwise → All plants run in NORMAL mode (1Hz aggregation)
    
    **Operating Modes:**
    - **NORMAL**: 60s aggregation window (98% bandwidth reduction)
    - **DEBUG**: Raw 50Hz passthrough (leak investigation)
    - **ECONOMY**: 300s aggregation (cloud congestion mitigation)
    """)

# Auto-refresh every 5 seconds
if time.time() - st.session_state.last_refresh > 5:
    st.session_state.last_refresh = time.time()
    st.rerun()
