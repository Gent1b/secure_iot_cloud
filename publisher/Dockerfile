FROM python:3.9-slim

WORKDIR /app

COPY device.py /app/

RUN pip install --no-cache-dir paho-mqtt prometheus_client

CMD ["python", "device.py"]
