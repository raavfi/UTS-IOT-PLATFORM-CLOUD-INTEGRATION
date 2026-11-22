# UTS IoT Platform & Cloud Integration — Smart Greenhouse

**Nama:** Rafi Almazzydane Fairuz Zain  
**NIM:** 24.83.1122  
**Mata Kuliah:** IoT Platform & Cloud Integration (TK122)  
**Repository:** `uts-iot-greenhouse-24831122`

## Deskripsi

Proyek Smart Greenhouse ini merupakan implementasi platform IoT untuk simulasi dan pemantauan:

- Suhu udara
- Kelembapan udara
- Kelembapan tanah
- Status dan kontrol pompa irigasi

Sistem menggunakan MQTT untuk komunikasi sensor dan aktuator, Apache Kafka untuk streaming data, Edge Gateway untuk agregasi dan Store & Forward, serta ThingsBoard CE untuk visualisasi dan RPC kontrol pompa.

## Arsitektur Sistem

```text
Device Simulator
       │ MQTT
       ▼
Mosquitto MQTT Broker
       │ MQTT
       ▼
Edge Gateway
   ├── Agregasi 1 menit
   ├── Deteksi suhu kritis
   ├── Deteksi tanah kering
   ├── SQLite Store & Forward
   ├── Apache Kafka ──► Kafka Consumer
   └── ThingsBoard MQTT ──► Dashboard dan RPC Pompa
```

Alur data:

1. `device_simulator.py` menghasilkan data simulasi dan mengirimnya melalui MQTT.
2. Mosquitto menerima topik sensor pada namespace `greenhouse/<NIM>/zona1/`.
3. Edge Gateway mengambil sensor yang masuk dan menghitung agregasi setiap 60 detik.
4. Edge Gateway mengirim agregasi ke Apache Kafka dan ThingsBoard.
5. Jika ThingsBoard tidak dapat dijangkau, data agregasi disimpan ke SQLite lalu dikirim ulang saat koneksi pulih.
6. RPC ThingsBoard `setPompa` diteruskan ke simulator melalui MQTT untuk mengubah status pompa.

## Struktur Repository

```text
UTS-IOT-PLATFORM-CLOUD-INTEGRATION/
├── README.md
└── uts-iot-greenhouse-24831122/
    ├── docker-compose.yml
    ├── consumer/
    │   └── kafka_consumer.py
    ├── edge/
    │   ├── edge_gateway.py
    │   └── edge_buffer.db
    ├── mosquitto/
    │   └── config/
    │       └── mosquitto.conf
    ├── simulator/
    │   └── device_simulator.py
    └── edge_buffer.db
```

> Saat penggunaan Edge Gateway berjalan dari folder proyek, `edge_buffer.db` dibuat atau digunakan sebagai buffer SQLite. Database tersebut tidak boleh dipublikasikan.

## Komponen yang Digunakan

| Komponen | Fungsi |
|---|---|
| Device Simulator | Menghasilkan suhu, kelembapan udara, kelembapan tanah, dan status pompa simulasi |
| Mosquitto | Broker MQTT lokal untuk komunikasi sensor, edge gateway, dan kontrol pompa |
| Edge Gateway | Mengolah, mengagregasi, meneruskan data, dan menyimpan buffer offline |
| Apache Kafka | Menyimpan stream telemetri pada topic Greenhouse |
| Kafka UI | Membantu inspeksi topic dan pesan Kafka |
| Kafka Consumer | Membaca dan menampilkan data telemetri dari Kafka |
| ThingsBoard CE | Penyimpanan, dashboard, alarm, dan RPC kontrol pompa |
| SQLite | Penyimpanan sementara pada mode Store & Forward |

## Persyaratan

- Docker Desktop
- Docker Compose
- Python 3.10 atau lebih baru
- PowerShell
- Library Python: `paho-mqtt`, `confluent-kafka`, dan `python-dotenv`

## Konfigurasi Environment

File `.env` dibuat secara lokal dan tidak boleh dipublikasikan ke GitHub.

Contoh konfigurasi aman:

```dotenv
NIM=24831122

MQTT_BROKER=localhost
MQTT_PORT=1883

KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=greenhouse-telemetry-24831122

TB_HOST=localhost
TB_MQTT_PORT=1884
TB_ACCESS_TOKEN=
```

Nilai `TB_ACCESS_TOKEN` diisi dengan access token device yang dibuat di ThingsBoard. Jangan menaruh token nyata pada README, kode, atau `.env.example`.

Buat `.env` dari terminal PowerShell:

```powershell
@"
NIM=24831122
MQTT_BROKER=localhost
MQTT_PORT=1883
KAFKA_BOOTSTRAP=localhost:9092
KAFKA_TOPIC=greenhouse-telemetry-24831122
TB_HOST=localhost
TB_MQTT_PORT=1884
TB_ACCESS_TOKEN=
"@ | Set-Content -Encoding utf8 .env
```

## Menjalankan Docker Compose

Masuk ke folder proyek:

```powershell
cd "<folder-repository>\uts-iot-greenhouse-24831122"
```

Bangun dan jalankan seluruh layanan:

```powershell
docker compose up -d --build
```

Periksa status:

```powershell
docker compose ps
```

Periksa log:

```powershell
docker compose logs -f
```

Hentikan layanan:

```powershell
docker compose down
```

Layanan yang tersedia:

- Mosquitto MQTT: port `1883`
- ThingsBoard HTTP: `http://localhost:8080`
- ThingsBoard MQTT: port `1884`
- Apache Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8090`

## Menjalankan Python Secara Manual

Buat dan aktifkan virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

Jika PowerShell membatasi eksekusi skrip:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Pasang dependency:

```powershell
pip install paho-mqtt confluent-kafka python-dotenv
```

Jalankan dari tiga terminal PowerShell yang berbeda:

```powershell
python .\simulator\device_simulator.py
```

```powershell
python .\edge\edge_gateway.py
```

```powershell
python .\consumer\kafka_consumer.py worker-1
```

Jalankan consumer tambahan untuk menguji consumer group:

```powershell
python .\consumer\kafka_consumer.py worker-2
```

Simulasi dapat dihentikan dengan `Ctrl+C`.

## Topik MQTT

Simulator menggunakan:

```text
greenhouse/24831122/zona1/status
greenhouse/24831122/zona1/suhu
greenhouse/24831122/zona1/kelembapan_udara
greenhouse/24831122/zona1/kelembapan_tanah
greenhouse/24831122/zona1/command/pompa
```

Perintah pompa dapat dikirim ke:

```text
greenhouse/24831122/zona1/command/pompa
```

Payload pompa:

```text
ON
OFF
```

Untuk memantau seluruh topik menggunakan Mosquitto subscriber:

```powershell
docker compose exec mosquitto mosquitto_sub -h localhost -t "greenhouse/24831122/zona1/#" -v
```

## Apache Kafka

Topik bawaan yang digunakan:

```text
greenhouse-telemetry-24831122
```

Kafka UI dapat dibuka pada:

```text
http://localhost:8090
```

Untuk melihat pesan melalui consumer Python:

```powershell
python .\consumer\kafka_consumer.py worker-1
```

## ThingsBoard CE

1. Jalankan Docker Compose.
2. Buka `http://localhost:8080`.
3. Gunakan akun ThingsBoard bawaan lalu ubah password sesuai kebutuhan.
4. Buat device dan ambil access token device.
5. Simpan token pada variabel `TB_ACCESS_TOKEN` di `.env`.
6. Buat dashboard dan tambahkan telemetry:
   - `suhu_avg`
   - `kelembapan_udara_avg`
   - `kelembapan_tanah_avg`
   - `sample_count`
7. Buat kontrol RPC yang memanggil method `setPompa` untuk mengubah pompa menjadi `ON` atau `OFF`.

## Mekanisme Store & Forward

Edge Gateway menyimpan agregasi ke SQLite ketika koneksi ke ThingsBoard tidak tersedia.

Ciri-ciri buffer aktif:

- Pesan `[SQLITE BUFFER]` muncul pada log.
- File `edge_buffer.db` terbuat atau bertambah ukurannya.
- Data dikirim ulang saat koneksi ThingsBoard berhasil kembali.

Database buffer hanya digunakan untuk pengujian lokal dan tidak boleh dikomit ke GitHub.

## Deteksi Kondisi

Edge Gateway menghasilkan alert lokal ketika:

- Suhu melebihi `33°C`
- Kelembapan tanah kurang dari `30%`

Aplikasi melakukan agregasi lima parameter MQTT menjadi satu payload telemetri setelah periode 60 detik jika semua kelompok sensor tersedia.

## Keamanan dan Kredensial

Jangan menyimpan atau mempublikasikan:

- Password
- Token ThingsBoard
- Token Telegram atau API key, jika digunakan
- Kredensial MQTT
- File `.env`
- Database SQLite atau file `*.db`
- Virtual environment
- Cache Python
- File log yang berisi rahasia

Tambahkan pola berikut ke `.gitignore`:

```gitignore
.env
.env.*
!.env.example
*.db
*.sqlite
*.sqlite3
edge_buffer.db
__pycache__/
*.py[cod]
.venv/
venv/
*.log
```

Periksa file yang sedang ditambahkan sebelum commit:

```powershell
git status
git diff --cached --name-only
git check-ignore -v .env edge_buffer.db
```

Cari pola kredensial pada staged files:

```powershell
git grep -n -E "(password|passwd|secret|api[_-]?key|access[_-]?token|bot[_-]?token)" -- . ':!*.md'
```

Jika kredensial telah terunggah, segera ganti atau revoke token lama dan bersihkan riwayat repository apabila diperlukan.

## Pengujian Sistem

- [ ] Docker Compose berhasil dijalankan.
- [ ] Mosquitto menerima data sensor.
- [ ] Simulator dapat menerima perintah pompa.
- [ ] Edge Gateway dapat membaca topik sensor.
- [ ] Agregasi satu menit berhasil dibuat.
- [ ] Data berhasil dikirim ke Kafka.
- [ ] Kafka Consumer menampilkan pesan.
- [ ] Data tampil pada ThingsBoard.
- [ ] RPC `setPompa` mengubah status pompa pada simulator.
- [ ] SQLite menyimpan data saat ThingsBoard terputus.
- [ ] Database dan `.env` tidak tersedia di GitHub.

## Video Demonstrasi

Tambahkan tautan video demonstrasi pada repository atau di bagian ini.

```text
https://drive.google.com/drive/folders/1XE3072FmOEQVhpq6jeEKfg4GvdQbUipp?usp=drive_link
```

## Lisensi

Proyek ini dibuat untuk keperluan tugas UTS mata kuliah IoT Platform & Cloud Integration (TK122).
