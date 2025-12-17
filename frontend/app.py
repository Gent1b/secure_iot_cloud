import os
import time
import json
import queue
import streamlit as st
import paho.mqtt.client as mqtt
import requests
from datetime import datetime

# =============================================================================
# CONFIGURATION
# =============================================================================
BROKER = "54.93.230.47"  # MQTT VM public IP (hardcoded for Docker connectivity)
PORT = 1883
TOPIC_CONTROL = "iot/control/#"
TOPIC_DATA = "iot/data/#"

PROMETHEUS_URL = os.getenv("PROMETHEUS_URL", "http://172.31.42.61:9090")
SUBSCRIBER_METRICS_URL = "http://subscriber:8000/metrics"

SITES = ["plant-a", "plant-b", "plant-c"]
SITE_LABELS = {"plant-a": "Plant A", "plant-b": "Plant B", "plant-c": "Plant C"}

# =============================================================================
# SESSION STATE INITIALIZATION
# =============================================================================
if "logs" not in st.session_state:
    st.session_state.logs = []
if "mqtt_client" not in st.session_state:
    st.session_state.mqtt_client = None
if "mqtt_connected" not in st.session_state:
    st.session_state.mqtt_connected = False
if "plant_modes" not in st.session_state:
    st.session_state.plant_modes = {site: "WAITING" for site in SITES}
if "leak_status" not in st.session_state:
    st.session_state.leak_status = {site: 0 for site in SITES}
if "last_update" not in st.session_state:
    st.session_state.last_update = {}

msg_queue = queue.Queue()

# =============================================================================
# MQTT CALLBACKS
# =============================================================================
def on_connect(client, userdata, flags, rc):
    """Called when MQTT connection is established"""
    if rc == 0:
        st.session_state.mqtt_connected = True
        client.subscribe(TOPIC_CONTROL)
        client.subscribe(TOPIC_DATA)
        msg_queue.put({
            "time": datetime.now(),
            "type": "system",
            "msg": f"✅ Connected to {BROKER} | Subscribed to topics"
        })
    else:
        st.session_state.mqtt_connected = False
        msg_queue.put({
            "time": datetime.now(),
            "type": "error",
            "msg": f"❌ Connection failed (RC={rc})"
        })

def on_message(client, userdata, msg):
    """Called when MQTT message is received"""
    try:
        topic = msg.topic
        payload = json.loads(msg.payload.decode())
        timestamp = datetime.now()
        
        # CONTROL MESSAGES: Mode changes from controller
        if topic.startswith("iot/control/"):
            site_id = topic.split("/")[-1]
            mode = payload.get("mode", "UNKNOWN")
            st.session_state.plant_modes[site_id] = mode
            st.session_state.last_update[site_id] = timestamp
            
            mode_emoji = {"DEBUG": "🔴", "NORMAL": "🟢", "ECONOMY": "🟡"}.get(mode, "⚪")
            msg_queue.put({
                "time": timestamp,
                "type": "control",
                "msg": f"{mode_emoji} {SITE_LABELS.get(site_id, site_id)} → {mode} mode",
                "site": site_id,
                "mode": mode
            })
        
        # DATA MESSAGES: Leak detection events
        elif topic.startswith("iot/data/"):
            site_id = topic.split("/")[-1]
            leak_flag = payload.get("leak_flag", 0)
            
            # Leak started
            if leak_flag == 1 and st.session_state.leak_status.get(site_id) != 1:
                st.session_state.leak_status[site_id] = 1
                msg_queue.put({
                    "time": timestamp,
                    "type": "alert",
                    "msg": f"🚨 {SITE_LABELS.get(site_id, site_id)}: LEAK DETECTED!",
                    "site": site_id
                })
            
            # Leak resolved
            elif leak_flag == 0 and st.session_state.leak_status.get(site_id) == 1:
                st.session_state.leak_status[site_id] = 0
                msg_queue.put({
                    "time": timestamp,
                    "type": "info",
                    "msg": f"✅ {SITE_LABELS.get(site_id, site_id)}: Leak resolved",
                    "site": site_id
                })
                
    except Exception as e:
        msg_queue.put({
            "time": datetime.now(),
            "type": "error",
            "msg": f"⚠️ Parse error: {str(e)[:50]}"
        })

def start_mqtt():
    """Initialize and start MQTT client"""
    if st.session_state.mqtt_client is None:
        try:
            client = mqtt.Client(client_id="thesis_dashboard")
            client.on_connect = on_connect
            client.on_message = on_message
            client.connect(BROKER, PORT, 60)
            client.loop_start()
            st.session_state.mqtt_client = client
        except Exception as e:
            st.session_state.mqtt_connected = False
            msg_queue.put({
                "time": datetime.now(),
                "type": "error",
                "msg": f"❌ MQTT connection failed: {str(e)[:50]}"
            })

# =============================================================================
# METRICS FUNCTIONS
# =============================================================================
def get_cloud_cpu():
    """Query Prometheus for Cloud Node CPU usage"""
    try:
        query = '100 - (avg(rate(node_cpu_seconds_total{job="cloud_node_exporter",mode="idle"}[1m])) * 100)'
        response = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={'query': query}, timeout=3)
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
        response = requests.get(SUBSCRIBER_METRICS_URL, timeout=3)
        if response.status_code == 200:
            lines = response.text.split('\n')
            metrics = {}
            for line in lines:
                if line.startswith('mqtt_messages_arrived_total'):
                    metrics['arrived'] = int(float(line.split()[1]))
                elif line.startswith('influxdb_writes_success_total'):
                    metrics['writes'] = int(float(line.split()[1]))
                elif line.startswith('pipeline_leaks_detected_total'):
                    metrics['leaks'] = int(float(line.split()[1]))
                elif line.startswith('iot_bandwidth_bytes_total'):
                    metrics['bandwidth'] = float(line.split()[1]) / (1024 * 1024)  # MB
            return metrics
    except:
        pass
    return {}

def get_mode_color(mode):
    """Get color for operating mode"""
    colors = {
        "DEBUG": "#dc3545",
        "NORMAL": "#28a745",
        "ECONOMY": "#ffc107",
        "WAITING": "#6c757d",
        "STALE": "#fd7e14"
    }
    return colors.get(mode, "#6c757d")

def publish_leak(site_id):
    """Inject a leak event for demonstration"""
    if st.session_state.mqtt_client and st.session_state.mqtt_connected:
        topic = f"iot/data/{site_id}"
        payload = {
            "pressure_psi": 25.0,
            "flow_gpm": 150.0,
            "tank_level": 75.0,
            "leak_flag": 1,
            "timestamp": time.time()
        }
        st.session_state.mqtt_client.publish(topic, json.dumps(payload))
        st.success(f"🔥 Injected leak event for {SITE_LABELS[site_id]}")

def publish_normal(site_id):
    """Send normal telemetry for demonstration"""
    if st.session_state.mqtt_client and st.session_state.mqtt_connected:
        topic = f"iot/data/{site_id}"
        payload = {
            "pressure_psi": 45.0,
            "flow_gpm": 100.0,
            "tank_level": 80.0,
            "leak_flag": 0,
            "timestamp": time.time()
        }
        st.session_state.mqtt_client.publish(topic, json.dumps(payload))
        st.success(f"✅ Sent normal telemetry for {SITE_LABELS[site_id]}")

# =============================================================================
# PAGE CONFIGURATION
# =============================================================================
st.set_page_config(
    page_title="IoT Feedback Loop - Thesis Demo",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        margin-bottom: 1.5rem;
        color: white;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    .mode-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        border: 2px solid #e0e0e0;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        text-align: center;
        transition: transform 0.2s;
    }
    .mode-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 4px 8px rgba(0,0,0,0.15);
    }
    .mode-badge {
        padding: 0.8rem 1.5rem;
        border-radius: 25px;
        font-weight: bold;
        font-size: 1.4rem;
        color: white;
        margin: 1rem 0;
        display: inline-block;
    }
    .log-entry {
        padding: 0.7rem 1rem;
        border-left: 4px solid #667eea;
        margin: 0.5rem 0;
        background: white;
        border-radius: 5px;
        box-shadow: 0 1px 3px rgba(0,0,0,0.1);
        font-family: 'Courier New', monospace;
    }
    .metric-box {
        background: white;
        padding: 1rem;
        border-radius: 8px;
        border-top: 4px solid #667eea;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)

# =============================================================================
# HEADER
# =============================================================================
st.markdown('''
<div class="main-header">
    <h1 style="margin: 0;">🔄 Secure IoT System with Edge Computing</h1>
    <p style="font-size: 1.1rem; margin: 0.5rem 0 0 0; opacity: 0.95;">
        Real-Time Feedback Loop Controller | Master's Thesis Demonstration
    </p>
</div>
''', unsafe_allow_html=True)

# =============================================================================
# SIDEBAR
# =============================================================================
with st.sidebar:
    st.markdown("## 🎮 Simulation Controls")
    st.caption("Trigger events to demonstrate the feedback loop")
    
    for site_id in SITES:
        with st.expander(f"📍 {SITE_LABELS[site_id]}", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                if st.button("🔥 LEAK", key=f"leak_{site_id}", use_container_width=True):
                    publish_leak(site_id)
            with col2:
                if st.button("✅ Normal", key=f"normal_{site_id}", use_container_width=True):
                    publish_normal(site_id)
    
    st.divider()
    
    if st.button("🔄 Reconnect MQTT", use_container_width=True):
        if st.session_state.mqtt_client:
            st.session_state.mqtt_client.loop_stop()
            st.session_state.mqtt_client.disconnect()
        st.session_state.mqtt_client = None
        st.session_state.mqtt_connected = False
        st.rerun()
    
    st.divider()
    st.markdown("### 📖 Operating Modes")
    st.markdown("""
    - 🔴 **DEBUG**: 50Hz raw data  
      ↳ Leak detection mode
    - 🟢 **NORMAL**: 1Hz aggregated  
      ↳ Standard operation
    - 🟡 **ECONOMY**: 0.2Hz throttled  
      ↳ Resource conservation
    """)
    
    st.divider()
    st.caption(f"**Broker:** {BROKER}:{PORT}")
    st.caption(f"**Auto-refresh:** Every 1 second")

# =============================================================================
# MAIN DASHBOARD
# =============================================================================

# Start MQTT connection
start_mqtt()

# Process message queue
while not msg_queue.empty():
    log_entry = msg_queue.get()
    st.session_state.logs.insert(0, log_entry)
    if len(st.session_state.logs) > 100:
        st.session_state.logs.pop()

# Connection status
if st.session_state.mqtt_connected:
    st.success(f"✅ **CONNECTED** to MQTT Broker `{BROKER}` | Subscribed to `{TOPIC_CONTROL}` and `{TOPIC_DATA}`")
else:
    st.error(f"❌ **DISCONNECTED** from MQTT Broker - Click 'Reconnect MQTT' in sidebar")

st.divider()

# =============================================================================
# CLOUD METRICS
# =============================================================================
st.subheader("☁️ Cloud Infrastructure Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

cloud_cpu = get_cloud_cpu()
metrics = get_subscriber_metrics()

with col1:
    st.markdown('<div class="metric-box">', unsafe_allow_html=True)
    if cloud_cpu is not None:
        delta = "🟢 Normal" if cloud_cpu < 50 else "🔴 High"
        st.metric("Cloud CPU", f"{cloud_cpu:.1f}%", delta=delta)
    else:
        st.metric("Cloud CPU", "N/A")
    st.markdown('</div>', unsafe_allow_html=True)

with col2:
    st.markdown('<div class="metric-box">', unsafe_allow_html=True)
    st.metric("Messages", f"{metrics.get('arrived', 0):,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col3:
    st.markdown('<div class="metric-box">', unsafe_allow_html=True)
    st.metric("DB Writes", f"{metrics.get('writes', 0):,}")
    st.markdown('</div>', unsafe_allow_html=True)

with col4:
    st.markdown('<div class="metric-box">', unsafe_allow_html=True)
    leaks = metrics.get('leaks', 0)
    st.metric("Leaks Detected", f"{leaks}", delta="⚠️ Alert" if leaks > 0 else "")
    st.markdown('</div>', unsafe_allow_html=True)

with col5:
    st.markdown('<div class="metric-box">', unsafe_allow_html=True)
    st.metric("Bandwidth", f"{metrics.get('bandwidth', 0):.2f} MB")
    st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# =============================================================================
# EDGE AGENT MODES
# =============================================================================
st.subheader("⚙️ Edge Agent Operating Modes (Feedback Loop Status)")
st.caption("Controller monitors system state and dynamically adjusts edge agent transmission modes")

col_a, col_b, col_c = st.columns(3)

for col, site_id in zip([col_a, col_b, col_c], SITES):
    with col:
        mode = st.session_state.plant_modes.get(site_id, "WAITING")
        color = get_mode_color(mode)
        
        # Check if stale
        if site_id in st.session_state.last_update:
            time_ago = (datetime.now() - st.session_state.last_update[site_id]).seconds
            if time_ago > 30:
                mode = "STALE"
                color = get_mode_color("STALE")
        else:
            time_ago = None
        
        mode_desc = {
            "DEBUG": "📊 High-fidelity mode (50Hz raw)",
            "NORMAL": "✅ Standard operation (1Hz avg)",
            "ECONOMY": "💤 Throttled mode (0.2Hz)",
            "WAITING": "⏳ No data received yet",
            "STALE": "⚠️ Connection lost"
        }
        
        st.markdown(f'<div class="mode-card">', unsafe_allow_html=True)
        st.markdown(f"**{SITE_LABELS[site_id]}**")
        st.markdown(f'<div class="mode-badge" style="background-color: {color};">{mode}</div>', unsafe_allow_html=True)
        st.caption(mode_desc.get(mode, "Unknown"))
        if time_ago is not None:
            st.caption(f"🕐 Updated {time_ago}s ago")
        st.markdown('</div>', unsafe_allow_html=True)

st.divider()

# =============================================================================
# ACTIVITY LOG
# =============================================================================
st.subheader("📡 Live Feedback Loop Activity")

if st.session_state.logs:
    st.caption(f"**Real-time events** • {len(st.session_state.logs)} total logged")
    
    for log in st.session_state.logs[:15]:
        log_type = log.get("type", "info")
        timestamp = log.get("time", datetime.now()).strftime("%H:%M:%S")
        message = log.get("msg", "Unknown")
        
        type_colors = {
            "control": "#667eea",
            "alert": "#dc3545",
            "info": "#28a745",
            "system": "#17a2b8",
            "error": "#fd7e14"
        }
        color = type_colors.get(log_type, "#6c757d")
        
        st.markdown(
            f'<div class="log-entry" style="border-left-color: {color};">'
            f'<strong style="color: {color};">[{timestamp}]</strong> {message}'
            f'</div>',
            unsafe_allow_html=True
        )
else:
    st.info("⏳ **Waiting for activity...** Make sure the dynamic scenario is deployed on the edge VM.")

st.divider()

# =============================================================================
# SYSTEM ARCHITECTURE
# =============================================================================
with st.expander("📐 System Architecture Overview", expanded=False):
    st.markdown("""
    ### Three-Layer Hierarchical Architecture
    
    **1. Edge Layer (Devices VM)**
    - 🌊 Water sensors (30 publishers across 3 plants)
    - ⚡ Edge agents (local aggregation & filtering)
    - 📡 Local MQTT broker (inter-pod communication)
    
    **2. Transport Layer (MQTT VM)**
    - 🔌 Central MQTT broker (Mosquitto)
    - 🌐 Gateway between edge and cloud
    
    **3. Cloud Layer (Cloud VM)**
    - 💾 InfluxDB (time-series storage)
    - 📥 Subscriber (data ingestion)
    - 🎯 Controller (feedback orchestration)
    - 📊 This dashboard (visualization)
    
    ### Feedback Loop Algorithm
    
    The controller implements **Max-Min Fairness**:
    1. Query InfluxDB for leak events (last 60s)
    2. Query Prometheus for cloud CPU utilization
    3. Apply priority rules:
       - **Leak Priority**: Sites with leaks → DEBUG, others → ECONOMY
       - **Cloud Capacity**: CPU > 80% → throttle to ECONOMY
       - **Resource Maximization**: CPU < 20% → all to DEBUG
       - **Default**: All sites → NORMAL
    4. Publish mode commands via MQTT every 10s
    """)

# Auto-refresh
time.sleep(1)
st.rerun()
