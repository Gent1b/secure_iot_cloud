FROM python:3.9-slim

WORKDIR /app

# Copy both subscriber.py (cloud) and edge_agent.py (edge K3s)
# Note: Same image used for both, but K3s overrides CMD to run edge_agent.py
COPY subscriber.py edge_agent.py /app/

RUN pip install paho-mqtt influxdb-client prometheus_client

# Default: Run cloud subscriber (K3s deployments override this)
CMD ["python", "-u", "subscriber.py"]
