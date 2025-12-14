# Baseline Scenario (Cloud-Centric)

This scenario represents the "Stable Baseline" of the experiment.
All raw data is sent from the Device Node to the Cloud Node. Processing happens centrally.

## Architecture
- **Publisher**: Runs on `device-node`. Generates Water Pressure/Flow data.
- **Subscriber**: Runs on `cloud-node`.
  - Mode: `CLOUD`
  - Action: Receives raw MQTT data, computes features for *indexing* but stores ALL raw data points to InfluxDB.

## How to Run
1. Ensure the `secure_iot_system_cloud` environment is active.
2. Set the environment variable `PROCESSING_MODE=CLOUD` (default).
3. Start the stack:
   ```powershell
   # On Cloud Node
   cd cloud-node
   docker-compose up -d
   ```

## Verification
- Check InfluxDB: `water_pipeline` measurement should have 10-50Hz data points.
- Check Network: High incoming traffic on `cloud-node`.
