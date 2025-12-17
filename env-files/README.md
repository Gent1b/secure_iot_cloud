# Environment Files

These files need to be deployed to `/opt/iot-env/` on each VM.

## Deployment

Run this command from the **local machine** after pulling updates:

```powershell
# Deploy environment files to VMs
scp env-files/cloud.env cloud:/opt/iot-env/cloud.env
scp env-files/monitor.env monitoring:/opt/iot-env/monitor.env
scp env-files/mqtt.env mqtt:/opt/iot-env/mqtt.env
```

Or use the updated `1_pull_updates.ps1` script which will handle this automatically.

## File Mapping

| File | Destination VM | Path |
|------|----------------|------|
| cloud.env | cloud (18.156.173.64) | /opt/iot-env/cloud.env |
| monitor.env | monitoring (3.127.68.11) | /opt/iot-env/monitor.env |
| mqtt.env | mqtt (54.93.230.47) | /opt/iot-env/mqtt.env |

## Configuration Details

### cloud.env
- Used by: InfluxDB, Subscriber, Controller
- Contains: InfluxDB credentials, MQTT broker address, Prometheus URL

### monitor.env
- Used by: Grafana
- Contains: Grafana admin credentials, InfluxDB datasource connection (internal IP 172.31.33.61:8086)

### mqtt.env
- Used by: Broker monitor (if enabled)
- Contains: MQTT broker internal IP

## Critical Settings

**InfluxDB Token/Org must match across all files:**
- Token: `local_token_123`
- Org: `secure_iot`
- Bucket: `iot_data`

**InfluxDB URL:**
- Internal Docker: `http://influxdb:8086` (for services on cloud VM)
- Cross-VM: `http://172.31.33.61:8086` (for monitoring VM → cloud VM)

**MQTT Broker:**
- Public IP: `54.93.230.47` (for cross-VM connections)
- Internal IP: `172.31.39.30` (for local monitoring)
