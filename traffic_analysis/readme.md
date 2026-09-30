# 🚦 Traffic Analysis Pipeline — Hướng dẫn Vận hành Backend

Module này chứa toàn bộ hệ thống đường ống dữ liệu (Data Pipeline) từ khâu tiếp nhận (Ingestion), lưu trữ đệm phân tán (Message Streaming & Object Storage Lake) đến khâu trích xuất, chuyển đổi và phân tích (ETL & Analytical Warehouse).

---

## 🏛️ Kiến trúc 2 luồng hoạt động độc lập

Hệ thống được thiết kế hỗ trợ song song hai môi trường:

```text
┌─────────────────────────────────────────────────────────────────────────────────┐
│  LUỒNG SẢN XUẤT (Production Cloud)           LUỒNG KIỂM THỬ (Mock / Testing)    │
│                                                                                 │
│  [Flutter Mobile App]                             [mock_producer.py]            │
│         │ POST /save_gps                                  │                     │
│         ▼                                                 │ Kafka Produce       │
│  [app.py - API Gateway]                                   │                     │
│         │ Kafka Produce                                   │                     │
│         ▼                                                 ▼                     │
│  ┌─────────────────────────────────────────────────────────────┐                │
│  │              Apache Kafka (gps_stream_topic)                │                │
│  └─────────────────────────────────────────────────────────────┘                │
│                                 │                                               │
│                                 ▼                                               │
│                       [ingestion_worker.py]                                     │
│                        (Kafka → Parquet)                                        │
│                                 │                                               │
│                                 ▼                                               │
│                      [MinIO S3 Data Lake]                                       │
│                      ├── prod/gps/year=.../*.parquet                            │
│                      └── mock/gps/year=.../*.parquet                            │
│                                 │                                               │
│            ┌────────────────────┴─────────────────────┐                         │
│            ▼                                          ▼                         │
│   [etl_worker.py]                            [etl_worker.py --local]            │
│   (Xử lý Data thật)                          (Xử lý Data giả lập)               │
│            │                                          │                         │
│            ▼                                          ▼                         │
│    [Neon Postgres ☁️]                        [TimescaleDB Docker 🏠]            │
│    (Database Cloud)                          (Database Local - Cổng 5433)       │
└─────────────────────────────────────────────────────────────────────────────────┘
```

---

## 📁 Cấu trúc Thư mục

```text
traffic_analysis/
├── app.py                      # Flask API Gateway tiếp nhận GPS từ thiết bị
├── docker-compose.yml          # Triển khai Kafka, MinIO, TimescaleDB
├── unified_schema.sql          # Lược đồ DDL cơ sở dữ liệu PostGIS hợp nhất
├── requirements.txt            # Danh sách thư viện Python cần thiết
├── .env.example                # File cấu hình mẫu các biến môi trường
├── .env                        # File biến môi trường thực tế (được gitignore)
├── readme.md                   # Tài liệu hướng dẫn riêng cho module backend
└── kafka/
    ├── ingestion_worker.py     # Consumer lấy dữ liệu từ Kafka ghi vào MinIO (Parquet)
    ├── etl_worker.py           # Worker định kỳ nạp Parquet vào DB và suy luận ùn tắc
    └── test/
        └── mock_producer.py    # Script mô phỏng dữ liệu GPS kiểm thử tải
```

---

## ⚙️ Cài đặt & Khởi chạy lần đầu

### Bước 1: Chuẩn bị môi trường Python
Khuyến nghị tạo môi trường ảo Python 3.10+:
```bash
python -m venv venv
# Trên Windows:
.\venv\Scripts\activate
# Trên Linux/macOS:
source venv/bin/activate

pip install -r requirements.txt
```

### Bước 2: Thiết lập biến môi trường `.env`
Sao chép file cấu hình mẫu và điều chỉnh thông tin nếu cần:
```bash
copy .env.example .env     # Windows PowerShell / CMD
# hoặc: cp .env.example .env (Linux/macOS)
```
> **Ghi chú bảo mật:** File `.env` chứa mật khẩu kết nối database và access key đã được đưa vào `.gitignore`, tuyệt đối không đẩy lên kho mã nguồn công khai.

### Bước 3: Khởi động cụm dịch vụ qua Docker Compose
```bash
docker compose up -d
```
Kiểm tra các container đang chạy:
- **Apache Kafka**: Cổng `9092` (Client) & `29092` (Internal)
- **MinIO Object Storage**: Cổng `9000` (S3 API) & `9001` (Web Console: http://localhost:9001, User/Pass: `admin` / `admin123`)
- **TimescaleDB (PostgreSQL 15 + PostGIS)**: Cổng `5433` (User: `admin`, Pass: `admin123`, DB: `traffic_analytics`)

### Bước 4: Khởi tạo Lược đồ Cơ sở dữ liệu (`unified_schema.sql`)

* **Khởi tạo trên Local Docker:**
```bash
docker cp unified_schema.sql traffic_db:/tmp/
docker exec -i traffic_db psql -U admin -d traffic_analytics -f /tmp/unified_schema.sql
```

* **Khởi tạo trên Neon Cloud:**
```bash
psql "postgresql://neondb_owner:<YOUR_PASSWORD>@ep-old-voice-a1mw52bq-pooler.ap-southeast-1.aws.neon.tech/neondb?sslmode=require" -f unified_schema.sql
```

---

## 🚀 Hướng dẫn Chạy Pipeline

### Cách 1: Chạy Luồng Kiểm Thử Giả Lập (Local Testing Flow)
Không cần cài đặt app di động, dùng script sinh dữ liệu giả lập 50 phương tiện di chuyển trên đường Phạm Văn Đồng:

```bash
# Terminal 1 — Ingestion Worker gom data ghi vào MinIO (chế độ local)
python kafka/ingestion_worker.py --local

# Terminal 2 — ETL Worker đọc Parquet và ghi vào TimescaleDB Local
python kafka/etl_worker.py --local

# Terminal 3 — Sinh dữ liệu giả lập đẩy vào Kafka
python kafka/test/mock_producer.py --vehicles 50 --interval 1.0
```

### Cách 2: Chạy Luồng Dữ Liệu Thật (Production Cloud Flow)
Dành cho dữ liệu thực tế thu thập từ ứng dụng Flutter `gps_collector_app`:

```bash
# Terminal 1 — Cổng Flask API Gateway nhận request từ thiết bị di động
python app.py

# Terminal 2 — Ingestion Worker ghi vào thư mục prod/ trên MinIO
python kafka/ingestion_worker.py

# Terminal 3 — ETL Worker nạp dữ liệu và suy luận trực tiếp lên Neon Cloud
python kafka/etl_worker.py
```

---

## 🔍 Kiểm tra Kết quả & Giám sát

1. **Kiểm tra trạng thái API Gateway:**
   Mở trình duyệt truy cập: `http://localhost:5000/health`

2. **Kiểm tra MinIO Data Lake:**
   Truy cập http://localhost:9001 (Tài khoản: `admin` / `admin123`). Kiểm tra bucket `traffic-lake`, xem các file `.parquet` được lưu theo cấu trúc cây thư mục thời gian.

3. **Kiểm tra kết quả phân tích ùn tắc trên Database Local:**
```bash
docker exec -i traffic_db psql -U admin -d traffic_analytics -c "
  SELECT traffic_light_id, time_window, avg_speed_kmh, vehicle_count, 
         congestion_label, is_inefficient, analysis_time 
  FROM congestion_analysis 
  ORDER BY analysis_time DESC 
  LIMIT 10;
"
```
