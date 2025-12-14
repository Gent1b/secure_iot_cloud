# Simple System Explanation

## What Is This System?

Imagine you have **water sensors** in different locations (like pumps and tanks) that need to send data to a **cloud database** for monitoring. Normally, all sensors would send ALL their data directly to the cloud, which wastes bandwidth and costs money.

This system adds a **"smart middle layer"** (the Edge) that processes data locally and only sends what's important to the cloud.

---

## The 3 Layers (Simple)

```
🏭 DEVICES (Edge)
   └─ Water sensors generate data 50 times per second
   └─ Edge computer processes it locally
        ↓
        
🌐 INTERNET (MQTT Broker)
   └─ Messages travel through the internet
        ↓
        
☁️ CLOUD
   └─ Database stores the important data
   └─ Controller tells edge what to do
```

---

## How It Works (Step by Step)

### Step 1: Sensors Generate Data
- **10 water sensors** measure pressure, flow, and levels
- They send data **50 times per second** (very fast!)
- This creates 500 messages per second locally

### Step 2: Edge Computer Processes Data
The Edge computer does 2 important jobs:

**Job 1: Detect Leaks Immediately**
- Checks every sensor reading for leaks
- If it finds a leak, it sends an ALERT to the cloud RIGHT AWAY
- No waiting!

**Job 2: Reduce Data Before Sending**
- Instead of sending 3,000 messages per minute...
- It calculates the average and sends just 1 message per minute
- **Saves 99.97% of bandwidth!**

### Step 3: Cloud Stores Data
- Receives the aggregated data (1 message/min instead of 3,000)
- Stores it in InfluxDB database
- You can view it in Grafana dashboards

### Step 4: Cloud Gives Feedback
The cloud controller monitors the system and adjusts behavior:

**If cloud is idle** (low CPU):
- "Hey edge, send me more detailed data for analysis!"
- Edge switches to sending more data

**If there's a leak**:
- "Hey edge at site A, I need ALL your data to diagnose this!"
- "Hey edge at sites B and C, send less data to save bandwidth"

**If cloud is overloaded**:
- "Everyone, slow down and send less data!"

---

## The Magic: 3 Operating Modes

| Mode | What Edge Does | Data Sent | When Used |
|------|---------------|-----------|-----------|
| **ECONOMY** | Averages 5 minutes of data | Very little | Cloud is busy |
| **NORMAL** | Averages 1 minute of data | Normal amount | Default |
| **DEBUG** | Sends everything raw | All data | Leak detected OR cloud is idle |

---

## Real-World Example

### Without Edge Processing (Old Way):
```
Sensor: "Pressure is 45 PSI" (send to cloud)
Sensor: "Pressure is 46 PSI" (send to cloud)
Sensor: "Pressure is 45 PSI" (send to cloud)
... 3,000 more messages ...

Total sent: 3,000 messages per minute
```

### With Edge Processing (New Way):
```
Sensor → Edge: "Pressure is 45 PSI"
Sensor → Edge: "Pressure is 46 PSI"
Sensor → Edge: "Pressure is 45 PSI"
... Edge collects all 3,000 messages ...

Edge calculates: "Average pressure = 45.3 PSI"
Edge → Cloud: "Average pressure = 45.3 PSI" (1 message)

Total sent: 1 message per minute
```

**Result: 99.97% less bandwidth used!**

---

## What About Emergencies?

If the edge detects a **leak**, it doesn't wait to aggregate:

```
Sensor: "Flow is TOO HIGH! Pressure is LOW!"
    ↓
Edge: "LEAK DETECTED!" (sends alert immediately)
    ↓
Cloud: "I see the leak! Edge, send me everything raw!"
    ↓
Edge switches to DEBUG mode: sends all data (50 Hz)
```

**Response time: Less than 1 second**

---

## Why Is This Important?

### Traditional Cloud-Only System:
- ❌ Sends 6.5 GB per day
- ❌ High cloud costs
- ❌ Slow leak detection (2+ seconds)
- ❌ Wastes bandwidth

### Your Edge-Cloud System:
- ✅ Sends only 2 MB per day (99.97% reduction)
- ✅ Low cloud costs
- ✅ Instant leak detection (local)
- ✅ Smart resource usage

---

## Current Status

### What's Working:
✅ Sensors are publishing data  
✅ Edge is detecting leaks  
✅ Cloud is receiving data  
✅ Controller is sending commands  

### What Needs Fixing:
🔴 Edge is stuck in DEBUG mode (sending too much data)  
🔴 Controller is telling edge to send ALL data when it should send aggregated data  

**Fix needed:** Change 1 line in the controller code

---

## How to Verify It's Working

### Check Edge Status:
```powershell
ssh devices "sudo kubectl get pods -n iot-edge"
```
You should see: `Running` for all pods

### Check Cloud Status:
```powershell
ssh cloud "docker ps"
```
You should see: `controller`, `subscriber`, `influxdb` containers

### Check Data:
Open InfluxDB at `http://<cloud-ip>:8086`  
You should see data in the `water_pipeline` measurement

### Check Dashboards:
Open Grafana at `http://<monitoring-ip>:3000`  
You should see graphs of pressure, flow, leaks

---

## Summary for Non-Technical People

**Question:** What does this system do?

**Answer:** It's like having a smart assistant at each water sensor location. Instead of calling you 50 times per second to report every tiny change, the assistant:
1. Watches for problems (leaks) and calls you IMMEDIATELY if there's an emergency
2. Otherwise, takes notes for 1 minute and then gives you a summary
3. If you need more details, you can tell the assistant to send everything

This saves you from getting thousands of calls per day while still keeping you informed about emergencies instantly.

**Question:** Why is this better?

**Answer:** 
- **Saves money** (99.97% less data to the cloud)
- **Faster emergency response** (local leak detection)
- **Smarter resource usage** (sends more data when cloud is idle, less when busy)

**Question:** What makes this a Master's Thesis project?

**Answer:**
1. **Edge Computing**: Processing data locally instead of in the cloud
2. **Adaptive Systems**: System changes behavior based on conditions
3. **Feedback Control**: Cloud tells edge what to do dynamically
4. **Resource Management**: Fairness algorithm allocates bandwidth
5. **Real-World Application**: Water infrastructure monitoring
