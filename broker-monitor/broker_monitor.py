import paho.mqtt.client as mqtt
from prometheus_client import Counter, start_http_server
import json

broker_data_messages = Counter(
    'broker_data_messages_received_total',
    'Actual MQTT data messages received on topic iot/devices'
)

def on_message(client, userdata, msg):
    try:
        json.loads(msg.payload.decode())
        broker_data_messages.inc()
    except:
        pass

def main():
    start_http_server(8001)
    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_message
    mqtt_client.connect("mqtt_broker", 1883, 60)
    mqtt_client.subscribe("iot/devices")
    print("Broker monitor running on :8001")
    mqtt_client.loop_forever()

if __name__ == "__main__":
    main()
