import time
import json
import random
import argparse
import paho.mqtt.client as mqtt

# Configuration
BROKER = "10.0.0.4" # MQTT Node Internal IP
PORT = 1883
TOPIC = "iot/data/plant-a" # Inject directly to Central Broker as if from Edge Agent

def get_leak_payload():
    """
    Generates a payload that triggers the Leak Detection logic
    in both Edge Agent and Cloud Subscriber.
    Logic: Flow > (Pressure * Valve * 0.05 + 15.0)
    """
    return {
        "device_id": "demo-injector",
        "site_id": "plant-a",
        "timestamp": time.time(),
        "pressure_psi": 10.0,    # Low Pressure
        "flow_gpm": 100.0,       # High Flow (Leak!)
        "valve_position": 100,   # Valve Open
        "pump_status": 1,
        "tank_level_pct": 50.0,
        "mode": "DEMO"
    }

def get_normal_payload():
    """
    Generates a normal payload.
    """
    return {
        "device_id": "demo-injector",
        "site_id": "plant-a",
        "timestamp": time.time(),
        "pressure_psi": 50.0,
        "flow_gpm": 250.0,       # Normal Flow (50 * 100 * 0.05 = 250)
        "valve_position": 100,
        "pump_status": 1,
        "tank_level_pct": 50.0,
        "mode": "DEMO"
    }

def main():
    parser = argparse.ArgumentParser(description="Thesis Demo Director")
    parser.add_argument("--action", choices=["leak", "normal", "loop"], required=True, help="Action to perform")
    parser.add_argument("--count", type=int, default=1, help="Number of messages to send")
    parser.add_argument("--broker", type=str, default="10.0.0.4", help="MQTT Broker Address")
    args = parser.parse_args()

    client = mqtt.Client(client_id="demo_director")
    try:
        print(f"Connecting to {args.broker}...")
        client.connect(args.broker, PORT, 60)
    except Exception as e:
        print(f"Failed to connect to {args.broker}: {e}")
        return

    print(f"Executing action: {args.action}")
    
    for i in range(args.count):
        if args.action == "leak":
            payload = get_leak_payload()
        elif args.action == "normal":
            payload = get_normal_payload()
        elif args.action == "loop":
            # Alternate every 10 messages
            if (i // 10) % 2 == 0:
                payload = get_normal_payload()
            else:
                payload = get_leak_payload()
        
        client.publish(TOPIC, json.dumps(payload))
        print(f"[{i+1}/{args.count}] Sent {args.action} payload to {TOPIC}")
        time.sleep(1)

    client.disconnect()
    print("Done.")

if __name__ == "__main__":
    main()
