FROM python:3.9-slim

WORKDIR /app

# Copy the monitor script into the container
COPY broker_monitor.py /app/

# Install dependencies (both paho-mqtt and prometheus_client)
RUN pip install --no-cache-dir paho-mqtt prometheus_client

# Expose the Prometheus metrics HTTP port
EXPOSE 8001

# Start the broker monitor script
CMD ["python", "broker_monitor.py"]
