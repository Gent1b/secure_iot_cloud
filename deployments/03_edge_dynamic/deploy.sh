#!/bin/bash
# deployments/03_edge_dynamic/deploy.sh
# Deploys the Dynamic Edge Scenario (Feedback Loop)

echo ">>> Deploying Dynamic Edge Scenario..."

RUN_ID_VALUE="${RUN_ID:-run_unknown}"
SCENARIO_VALUE="${SCENARIO:-adaptive}"

# Reuse the static deployment logic for the base stack
../02_edge_static/deploy.sh

# Override tags for dynamic scenario
sudo kubectl -n iot-edge set env deployment/edge-agent-site-a RUN_ID="$RUN_ID_VALUE" SCENARIO="$SCENARIO_VALUE"
sudo kubectl -n iot-edge set env deployment/water-sensors-site-a RUN_ID="$RUN_ID_VALUE" SCENARIO="$SCENARIO_VALUE"

# Apply any dynamic-specific overrides if needed
# (For now, the dynamic logic is inside the Edge Agent code which is already deployed)

echo "✅ Dynamic Edge Deployed."
