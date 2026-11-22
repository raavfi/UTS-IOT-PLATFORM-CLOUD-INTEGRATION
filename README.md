# UTS IoT Platform & Cloud Integration — Smart Greenhouse

**Nama:** Rafi Almazzydane Fairuz Zain  
**NIM:** 24.83.1122  
**Mata Kuliah:** IoT Platform & Cloud Integration (TK122)  
**Studi Kasus:** Smart Greenhouse Monitoring dan Kontrol Pompa

## Deskripsi

Proyek ini merupakan implementasi sistem Smart Greenhouse berbasis IoT untuk memantau:

- Suhu udara
- Kelembapan udara
- Kelembapan tanah
- Status pompa

Sensor dan aktuator disimulasikan menggunakan MQTT, kemudian data diproses oleh Edge Gateway dan dikirim ke Apache Kafka. Data diproses lebih lanjut dan dikirim ke ThingsBoard CE untuk visualisasi. Sistem dapat mengirim notifikasi Telegram ketika kondisi tertentu terpenuhi.

## Arsitektur Sistem

```text
Simulator MQTT
      │
      ▼
Edge Gateway
      │
      ▼
Apache Kafka
      │
      ▼
ThingsBoard CE
      │
      ▼
Telegram
