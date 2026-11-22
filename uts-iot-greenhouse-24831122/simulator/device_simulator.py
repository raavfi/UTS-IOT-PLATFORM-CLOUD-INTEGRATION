import os, time, json, random
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from dotenv import load_dotenv

load_dotenv()
NIM = os.getenv("NIM", "24831122")
BROKER = os.getenv("MQTT_BROKER", "localhost")
PORT = int(os.getenv("MQTT_PORT", 1883))

DEVICE_ID = f"GH-NODE01-{NIM}"
BASE_TOPIC = f"greenhouse/{NIM}/zona1"
TOPIC_STATUS = f"{BASE_TOPIC}/status"
TOPIC_CMD_POMPA = f"{BASE_TOPIC}/command/pompa"

pompa_aktif = False

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[{DEVICE_ID}] Terhubung ke Mosquitto (rc={rc})")
    client.publish(TOPIC_STATUS, json.dumps({"device_id": DEVICE_ID, "status": "online"}), qos=1, retain=True)
    client.subscribe(TOPIC_CMD_POMPA, qos=1)

def on_message(client, userdata, msg):
    global pompa_aktif
    payload = msg.payload.decode()
    print(f"\n[COMMAND DITERIMA] Topic: {msg.topic} | Payload: {payload}")
    if "ON" in payload.upper() or "TRUE" in payload.upper():
        pompa_aktif = True
        print(">>> AKTUATOR: Pompa Irigasi MENYALA <<<\n")
    else:
        pompa_aktif = False
        print(">>> AKTUATOR: Pompa Irigasi MATI <<<\n")

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=DEVICE_ID)
lwt_payload = json.dumps({"device_id": DEVICE_ID, "status": "offline"})
client.will_set(TOPIC_STATUS, payload=lwt_payload, qos=1, retain=True)

client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER, PORT, keepalive=30)
client.loop_start()

try:
    step = 0
    while True:
        step += 1
        if step % 6 == 0:
            suhu = round(random.uniform(34.5, 37.0), 2)
            kelembapan_tanah = round(random.uniform(20.0, 27.0), 2)
        else:
            suhu = round(random.uniform(25.0, 30.5), 2)
            kelembapan_tanah = round(random.uniform(45.0, 65.0), 2)
        
        kelembapan_udara = round(random.uniform(60.0, 80.0), 2)
        ts = datetime.now(timezone.utc).isoformat()

        client.publish(f"{BASE_TOPIC}/suhu", json.dumps({"device_id": DEVICE_ID, "value": suhu, "unit": "C", "timestamp": ts}), qos=1)
        client.publish(f"{BASE_TOPIC}/kelembapan_udara", json.dumps({"device_id": DEVICE_ID, "value": kelembapan_udara, "unit": "%", "timestamp": ts}), qos=1)
        client.publish(f"{BASE_TOPIC}/kelembapan_tanah", json.dumps({"device_id": DEVICE_ID, "value": kelembapan_tanah, "unit": "%", "timestamp": ts}), qos=1)

        print(f"[PUBLISH] Suhu={suhu}C | Udara={kelembapan_udara}% | Tanah={kelembapan_tanah}% | Pompa={'ON' if pompa_aktif else 'OFF'}")
        time.sleep(5)
except KeyboardInterrupt:
    print("Simulator dihentikan secara normal.")
    client.publish(TOPIC_STATUS, lwt_payload, qos=1, retain=True)
    client.disconnect()
