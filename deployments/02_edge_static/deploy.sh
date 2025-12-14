#!/bin/bash
# deployments/02_edge_static/deploy.sh
# Deploys the Static Edge Scenario to K3s

echo ">>> Deploying Static Edge Scenario..."

# 1. Ensure K3s is running
if ! systemctl is-active --quiet k3s; then
    echo "K3s is not running. Installing/Starting..."
    curl -sfL https://get.k3s.io | sh -
fi

# 2. Build Images Locally (since we are using local images in K3s)
# Note: In a real prod env, we would pull from GHCR. 
# For this thesis, we build on the node to ensure latest code is used without pushing to registry every time.
echo "Building Docker images for K3s..."
# We need to import images into K3s containerd
# Or simpler: Use the GHCR images if the CI pipeline built them.
# Let's assume we use the GHCR images for stability, or build local if needed.

# For now, let's try to use the local build approach for rapid dev
docker build -t iot-subscriber:local ../../subscriber
docker build -t iot-publisher:local ../../publisher

# Export to K3s (k3s uses containerd, not docker daemon by default)
echo "Importing images to K3s..."
docker save iot-subscriber:local | sudo k3s ctr images import -
docker save iot-publisher:local | sudo k3s ctr images import -

# 3. Apply Manifests
echo "Applying Kubernetes Manifests..."
sudo kubectl apply -f k3s/edge-stack.yaml
sudo kubectl apply -f k3s/publishers.yaml

echo "✅ Static Edge Deployed. Check status with: sudo kubectl get pods -n iot-edge"
