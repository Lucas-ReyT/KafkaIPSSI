import sys
from kafka import KafkaConsumer
import json

if len(sys.argv) != 2:
    print("Usage: python consumer.py <topic>")
    sys.exit(1)

topic = sys.argv[1]

consumer = KafkaConsumer(
    topic,
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

print(f"📡 Listening to topic: {topic}")

for message in consumer:
    print(" Message reçu :", message.value)

