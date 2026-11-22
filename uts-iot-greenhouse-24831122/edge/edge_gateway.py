import os, time, json, sqlite3
from datetime import datetime, timezone
import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
NIM = os.getenv("NIM", "24831122")
LOCAL_BROKER = os.getenv("MQTT_BROKER", "localhost")
LOCAL_PORT = int(os.getenv("MQTT_PORT", 1883))

TB_HOST = os.getenv("TB_HOST", "localhost")
TB_PORT = int(os.getenv("TB_MQTT_PORT", 1884))
TB_TOKEN = os.getenv("TB_ACCESS_TOKEN")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", f"greenhouse-telemetry-{NIM}")

conn = sqlite3.connect("edge_buffer.db", check_same_thread=False)
conn.execute("CREATE TABLE IF NOT EXISTS buffer (id INTEGER PRIMARY KEY AUTOINCREMENT, payload TEXT)")
conn.commit()

kafka_producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})

window_data = {"suhu": [], "kelembapan_udara": [], "kelembapan_tanah": []}
last_flush_time = time.time()

tb_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"EDGE-TB-{NIM}")
tb_client.username_pw_set(TB_TOKEN)
tb_connected = False

def on_tb_connect(client, userdata, flags, rc, properties=None):
    global tb_connected
    if rc == 0:
        tb_connected = True
        print("[EDGE -> TB] Terhubung ke ThingsBoard!")
        client.subscribe("v1/devices/me/rpc/request/+")
        flush_sqlite_buffer()

def on_tb_disconnect(client, userdata, disconnect_flags, rc, properties=None):
    global tb_connected
    tb_connected = False
    print("[EDGE -> TB] Koneksi terputus! Mode Store & Forward aktif.")

def on_tb_message(client, userdata, msg):
    data = json.loads(msg.payload.decode())
    print(f"[RPC DARI THINGSBOARD] {data}")
    if data.get("method") == "setPompa":
        status = "ON" if data.get("params") else "OFF"
        local_client.publish(f"greenhouse/{NIM}/zona1/command/pompa", status, qos=1)

tb_client.on_connect = on_tb_connect
tb_client.on_disconnect = on_tb_disconnect
tb_client.on_message = on_tb_message
tb_client.connect_async(TB_HOST, TB_PORT, keepalive=30)
tb_client.loop_start()

def flush_sqlite_buffer():
    cur = conn.cursor()
    cur.execute("SELECT id, payload FROM buffer ORDER BY id ASC")
    rows = cur.fetchall()
    if rows:
        print(f"[STORE & FORWARD] Mengirim ulang {len(rows)} data tertunda dari SQLite...")
        for row_id, payload_str in rows:
            res = tb_client.publish("v1/devices/me/telemetry", payload_str, qos=1)
            if res.rc == mqtt.MQTT_ERR_SUCCESS:
                conn.execute("DELETE FROM buffer WHERE id = ?", (row_id,))
        conn.commit()

local_client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"EDGE-GW-{NIM}")

def on_local_connect(client, userdata, flags, rc, properties=None):
    client.subscribe(f"greenhouse/{NIM}/zona1/+", qos=1)
    print(f"[EDGE] Mendengarkan sensor lokal pada greenhouse/{NIM}/zona1/+")

def on_local_message(client, userdata, msg):
    global last_flush_time
    topic_end = msg.topic.split("/")[-1]
    if topic_end not in window_data:
        return

    data = json.loads(msg.payload.decode())
    val = data["value"]
    window_data[topic_end].append(val)

    if topic_end == "suhu" and val > 33.0:
        print(f"[EDGE LOCAL ALERT] Suhu kritis terdeteksi: {val}C!")
    if topic_end == "kelembapan_tanah" and val < 30.0:
        print(f"[EDGE LOCAL ALERT] Tanah terlalu kering: {val}%!")

    if time.time() - last_flush_time >= 60:
        if all(len(v) > 0 for v in window_data.values()):
            agg_payload = {
                "device_id": f"GH-NODE01-{NIM}",
                "suhu_avg": round(sum(window_data["suhu"]) / len(window_data["suhu"]), 2),
                "kelembapan_udara_avg": round(sum(window_data["kelembapan_udara"]) / len(window_data["kelembapan_udara"]), 2),
                "kelembapan_tanah_avg": round(sum(window_data["kelembapan_tanah"]) / len(window_data["kelembapan_tanah"]), 2),
                "sample_count": len(window_data["suhu"]),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
            payload_str = json.dumps(agg_payload)
            print(f"\n[EDGE AGREGASI 1 MENIT] {payload_str}")

            if tb_connected:
                tb_client.publish("v1/devices/me/telemetry", payload_str, qos=1)
            else:
                conn.execute("INSERT INTO buffer (payload) VALUES (?)", (payload_str,))
                conn.commit()
                print("[SQLITE BUFFER] Data disimpan lokal karena koneksi cloud terputus.")

            kafka_producer.produce(KAFKA_TOPIC, key=agg_payload["device_id"], value=payload_str)
            kafka_producer.flush()
            print(f"[EDGE -> KAFKA] Terkirim ke topic {KAFKA_TOPIC}\n")

        for k in window_data:
            window_data[k] = []
        last_flush_time = time.time()

local_client.on_connect = on_local_connect
local_client.on_message = on_local_message
local_client.connect(LOCAL_BROKER, LOCAL_PORT, keepalive=30)
local_client.loop_forever()
