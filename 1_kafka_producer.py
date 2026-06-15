from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Connect to Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

machine_id = "M23793"
print(f"Starting infinite live stream for {machine_id}...")

try:
    while True:
        # Base normal operation (Green)
        torque = random.uniform(35.0, 48.0)
        rpm = random.randint(1450, 1600)

        # 10% chance to simulate a Warning (Yellow)
        if random.random() < 0.10:
            torque = random.uniform(51.0, 58.0)

        # 5% chance to simulate High Risk (Red)
        if random.random() < 0.05:
            torque = random.uniform(61.0, 70.0)
            rpm = random.randint(1200, 1350)

        # Build the payload matching your PySpark schema
        data = {
            "UDI": random.randint(10000, 99999),
            "Product ID": machine_id,
            "Type": "M",
            "Air temperature [K]": 298.1,
            "Process temperature [K]": 308.6,
            "Rotational speed [rpm]": rpm,
            "Torque [Nm]": torque,
            "Tool wear [min]": random.randint(0, 200),
            "Target": 0,
            "Failure Type": "No Failure"
        }

        # Send to Kafka
        producer.send('telemetry_stream', data)
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Sent: Torque={torque:.1f}, RPM={rpm}")
        
        # Wait 2 seconds before sending the next reading
        time.sleep(2) 

except KeyboardInterrupt:
    print("\nStream stopped by user.")
finally:
    producer.close()
