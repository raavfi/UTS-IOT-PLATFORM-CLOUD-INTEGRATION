import os, sys
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()
NIM = os.getenv("NIM", "24831122")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", f"greenhouse-telemetry-{NIM}")
WORKER_NAME = sys.argv[1] if len(sys.argv) > 1 else "worker-1"

conf = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": f"greenhouse-analytics-group-{NIM}",
    "auto.offset.reset": "earliest"
}

consumer = Consumer(conf)
consumer.subscribe([KAFKA_TOPIC])
print(f"[{WORKER_NAME}] Menunggu data pada topic {KAFKA_TOPIC}...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None or msg.error():
            continue
        print(f"[{WORKER_NAME}] Partition={msg.partition()} | Offset={msg.offset()} | Data={msg.value().decode()}")
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
