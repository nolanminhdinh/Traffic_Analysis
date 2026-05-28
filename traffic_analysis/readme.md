# Traffic Analysis Pipeline — Hướng dẫn chạy

## Kiến trúc 2 luồng song song

```
┌─────────────────────────────────────────────────────────────────┐
│  LUỒNG THẬT (Production)          LUỒNG MOCK (Testing)          │
│                                                                 │
│  [Flutter App]                    [mock_producer.py]             │
│       │ POST /save_gps                  │ Kafka produce          │
│       ▼                                 ▼                        │
│  [app.py - Flask]          ────►  [Apache Kafka]  ◄────          │
│       │ produce                         │                        │
│       ▼                                 │                        │
│  [Apache Kafka]                         │                        │
│       │                                 │                        │
│       └──────────┬──────────────────────┘                        │
│                  ▼                                              │
│         [ingestion_worker.py]                                   │
│           Kafka → Parquet → MinIO                               │
│                  │                                              │
│         ┌────────┴────────┐                                     │
│         ▼                 ▼                                     │
│  etl_worker.py     etl_worker.py --local                        │
│  (CLOUD Neon)      (LOCAL Docker)                               │
│         │                 │                                     │
│         ▼                 ▼                                     │
│  [Neon DB ☁️]       [PostgreSQL 🏠]                              │
│  Data thật          Data mock/test                              │
│  Schema đồng bộ ←→ Schema đồng bộ                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## Schema hợp nhất — `unified_schema.sql`

Cả 2 DB dùng **chung 1 file schema**. Các bảng:

| Bảng | Mô tả |
|---|---|
| `intersections` | Nút giao thông tĩnh |
| `traffic_lights` | Đèn giao thông + tọa độ + cài đặt mặc định |
| `light_schedule` | Lịch điều khiển đèn theo khung giờ |
| `time_dimension` | Chiều thời gian (theo giờ, có time_window, season) |
| `vehicle_gps` | GPS thực tế từ app (partitioned theo tháng) |
| `congestion_analysis` | Kết quả phân tích ETL |

---

## Setup lần đầu

### 1. Khởi động Docker (Kafka + MinIO + PostgreSQL local)
```bash
docker compose up -d
sleep 20
```

### 2. Chạy schema lên CẢ 2 database

**Neon Cloud:**
```bash
psql "***" -f unified_schema.sql
```

**Local Docker:**
```bash
docker cp unified_schema.sql traffic_db:/tmp/
docker exec -i traffic_db psql -U admin -d traffic_analytics -f /tmp/unified_schema.sql
```

### 3. Cài Python dependencies
```bash
pip install -r requirements.txt
```

---

## Chạy pipeline hằng ngày

### Luồng THẬT (data từ Flutter App → Neon Cloud)

```bash
# Terminal 1 — Flask API Gateway
python app.py

# Terminal 2 — Ingestion Worker (Kafka → MinIO)
python ingestion_worker.py

# Terminal 3 — ETL → Neon Cloud
python etl_worker.py
```

Mở Flutter App → nhấn **Bắt đầu thu thập**.

---

### Luồng MOCK (data giả lập → Local Docker)

```bash
# Terminal 1 — Mock Producer (không cần app.py)
python mock_producer.py

# Terminal 2 — Ingestion Worker (dùng chung với luồng thật)
python ingestion_worker.py --local #gắn cờ local để tách giữa dữ liệu thật và giữa liệu giả lập 

# Terminal 3 — ETL → Local DB
python etl_worker.py --local #gắn cờ xác định đọc dữ liệu giả lập và lưu nó tại database trên local
```

---

## Chạy song song CẢ HAI luồng cùng lúc

```bash
# Terminal 1: Flask cho app thật
python app.py

# Terminal 2: Mock producer cho test
python mock_producer.py

# Terminal 3: Ingestion worker (xử lý cả 2 nguồn)
python ingestion_worker.py

# Terminal 4: ETL → Neon Cloud (data thật)
python etl_worker.py

# Terminal 5: ETL → Local (data mock)
python etl_worker.py --local
```

> **Lưu ý:** ingestion_worker dùng `group.id` riêng → cả 2 ETL worker đều nhận đủ data từ Kafka.

---

## Kiểm tra kết quả



**Local Docker:**
```bash
docker exec -i traffic_db psql -U admin -d traffic_analytics \
  -c "SELECT traffic_light_id, time_window, congestion_label, congestion_level, is_inefficient, analysis_time FROM congestion_analysis ORDER BY analysis_time DESC LIMIT 10;"
```

