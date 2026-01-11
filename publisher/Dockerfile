FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir paho-mqtt

# Copy Code
COPY device.py .

# Run the water pump simulation
CMD ["python", "-u", "device.py"]