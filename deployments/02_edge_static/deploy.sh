#!/bin/bash
# deployments/02_edge_static/deploy.sh
# Deploys the Static Edge Scenario to K3s

# Resolve the directory of this script to ensure paths are correct regardless of call location
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

echo ">>> Deploying Static Edge Scenario..."

# 1. Ensure K3s is running
if ! systemctl is-active --quiet k3s; then
    echo "K3s is not running. Installing/Starting..."
    curl -sfL https://get.k3s.io | sh -
fi

# Wait for K3s socket to be ready
echo "Waiting for K3s socket..."
for i in {1..30}; do
    if [ -S /run/k3s/containerd/containerd.sock ]; then
        echo "K3s socket is ready."
        break
    fi
    echo "Waiting for K3s..."
    sleep 2
done

# Double check with a simple command to ensure API is responsive
for i in {1..10}; do
    if sudo k3s ctr images list > /dev/null 2>&1; then
        echo "K3s containerd is responsive."
        break
    fi
    echo "Waiting for containerd API..."
    sleep 2
done

# 2. Build Images Locally (since we are using local images in K3s)
# Note: In a real prod env, we would pull from GHCR. 
# For this thesis, we build on the node to ensure latest code is used without pushing to registry every time.
echo "Building Docker images for K3s..."
# We need to import images into K3s containerd
# Or simpler: Use the GHCR images if the CI pipeline built them.
# Let's assume we use the GHCR images for stability, or build local if needed.

# For now, let's try to use the local build approach for rapid dev
# Use absolute paths relative to the script location
docker build -t iot-subscriber:local "$SCRIPT_DIR/../../subscriber"
docker build -t iot-publisher:local "$SCRIPT_DIR/../../publisher"

# Export to K3s (k3s uses containerd, not docker daemon by default)
echo "Importing images to K3s (using temp files to save memory)..."

# Function to safely import
safe_import() {
    IMG_NAME=$1
    # Sanitize filename (replace : with _)
    SAFE_NAME=$(echo "$IMG_NAME" | sed 's/:/_/g')
    
    echo ">>> Processing $IMG_NAME..."
    echo "  [1/3] Saving Docker image to disk..."
    docker save "$IMG_NAME" -o "/tmp/${SAFE_NAME}.tar"
    
    echo "  [2/3] Importing into K3s containerd..."
    sudo k3s ctr images import "/tmp/${SAFE_NAME}.tar"
    
    echo "  [3/3] Cleaning up temp file..."
    rm "/tmp/${SAFE_NAME}.tar"
    echo "✅ $IMG_NAME imported successfully."
}

safe_import "iot-subscriber:local"
safe_import "iot-publisher:local"

# 3. Apply Manifests
echo "Applying Kubernetes Manifests..."
sudo kubectl apply -f "$SCRIPT_DIR/k3s/edge-stack.yaml"
sudo kubectl apply -f "$SCRIPT_DIR/k3s/publishers.yaml"

echo "✅ Static Edge Deployed. Check status with: sudo kubectl get pods -n iot-edge"
