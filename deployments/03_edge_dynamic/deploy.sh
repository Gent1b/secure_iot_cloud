#!/bin/bash
# deployments/03_edge_dynamic/deploy.sh
# Deploys the Dynamic Edge Scenario (Feedback Loop)

echo ">>> Deploying Dynamic Edge Scenario..."

# Reuse the static deployment logic for the base stack
../02_edge_static/deploy.sh

# Apply any dynamic-specific overrides if needed
# (For now, the dynamic logic is inside the Edge Agent code which is already deployed)

echo "✅ Dynamic Edge Deployed."
