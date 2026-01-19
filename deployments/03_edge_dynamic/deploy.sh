#!/bin/bash
# deployments/03_edge_dynamic/deploy.sh
# Deploys the Dynamic Edge Scenario (Feedback Loop)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo ">>> Deploying Dynamic Edge Scenario..."

RUN_ID_VALUE="${RUN_ID:-run_unknown}"
SCENARIO_VALUE="${SCENARIO:-adaptive}"

# Load centralized env for broker IP
if [ -f /opt/iot/.env ]; then
	set -a
	# shellcheck disable=SC1091
	source /opt/iot/.env
	set +a
fi

BROKER_VALUE="${BROKER:-}"
DEVICES_VALUE="${DEVICES_PER_CONTAINER:-10}"
RATE_HZ_VALUE="${TARGET_RATE_HZ:-50}"
LEAK_SEED_VALUE="${LEAK_SEED:-42}"

# Reuse the static deployment logic for the base stack
# Call via bash to avoid relying on executable bit.
bash "$SCRIPT_DIR/../02_edge_static/deploy.sh"

# Override tags for dynamic scenario
sudo kubectl -n iot-edge set env deployment/edge-agent-site-a RUN_ID="$RUN_ID_VALUE" SCENARIO="$SCENARIO_VALUE"
sudo kubectl -n iot-edge set env deployment/water-sensors-site-a RUN_ID="$RUN_ID_VALUE" SCENARIO="$SCENARIO_VALUE" DEVICES_PER_CONTAINER="$DEVICES_VALUE" TARGET_RATE_HZ="$RATE_HZ_VALUE" LEAK_SEED="$LEAK_SEED_VALUE"

# Ensure edge agent uses centralized broker IP
if [ -n "$BROKER_VALUE" ]; then
	sudo kubectl -n iot-edge set env deployment/edge-agent-site-a CENTRAL_BROKER="$BROKER_VALUE"
else
	echo "WARNING: BROKER not set in /opt/iot/.env; edge-agent CENTRAL_BROKER not updated."
fi

# Apply any dynamic-specific overrides if needed
# (For now, the dynamic logic is inside the Edge Agent code which is already deployed)

echo "✅ Dynamic Edge Deployed."
