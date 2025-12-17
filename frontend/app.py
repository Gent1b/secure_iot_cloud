import streamlit as st
import paho.mqtt.client as mqtt
import json
import time
import threading
import os
import pandas as pd
import queue

# ---------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------
BROKER = os.getenv("BROKER", "172.31.39.30")
PORT = 1883
TOPIC_CONTROL = "iot/control/#"
TOPIC_DATA = "iot/data/#"

# ---------------------------------------------------------------------
# State Management
# ---------------------------------------------------------------------
if "logs" not in st.session_state:
    st.session_state.logs = []
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None

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
        print(f"Error: {e}")

def start_mqtt():
    if st.session_state.mqtt_client is None:
        client = mqtt.Client(client_id="frontend_dashboard")
        try:
            client.connect(BROKER, PORT, 60)
            client.subscribe(TOPIC_CONTROL)
            client.subscribe(TOPIC_DATA)
            client.on_message = on_message
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

# ---------------------------------------------------------------------
# UI Layout
# ---------------------------------------------------------------------
st.set_page_config(page_title="IoT Thesis Controller", layout="wide")

st.title("🚰 Secure IoT System: Feedback Loop Controller")
st.markdown("### Master's Thesis Demonstration")

# Sidebar for Controls
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

    st.divider()
    if st.button("Reconnect MQTT"):
        start_mqtt()

# Main Dashboard
start_mqtt()

# Process incoming messages
while not msg_queue.empty():
    st.session_state.logs.insert(0, msg_queue.get())
    # Keep log size manageable
    if len(st.session_state.logs) > 50:
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
