FROM python:3.9-slim

WORKDIR /app

COPY subscriber.py /app/

RUN pip install paho-mqtt influxdb-client prometheus_client

CMD ["python", "-u", "subscriber.py"]
