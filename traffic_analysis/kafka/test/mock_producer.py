"""
mock_producer.py — Sinh dữ liệu giả lập (Phiên bản Chuẩn Hóa NCKH)
══════════════════════════════════════════════════════════════
Đẩy data vào Kafka → ingestion_worker → MinIO
→ etl_worker --local → PostgreSQL LOCAL

Mô phỏng chính xác luồng dữ liệu Production sau khi qua API Gateway:
  gps_id, timestamp, latitude, longitude,
  speed_kmh, traffic_light_id, day_type, time_window
══════════════════════════════════════════════════════════════
"""
import json
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

# Khởi tạo Producer kết nối vào Kafka Docker
producer = Producer({"bootstrap.servers": "localhost:9092"})

# Danh mục nút giao trên tuyến Phạm Văn Đồng khớp với Lược đồ DB
TRAFFIC_LIGHTS = [
    {"id": "TL001", "name": "Ngã tư Xuân Đỉnh",      "lat": 21.0664, "lon": 105.7815},
    {"id": "TL002", "name": "Ngã tư Cổ Nhuế",          "lat": 21.0562, "lon": 105.7823},
    {"id": "TL003", "name": "Ngã tư Hoàng Quốc Việt",  "lat": 21.0458, "lon": 105.7831},
    {"id": "TL004", "name": "Ngã tư Mai Dịch",          "lat": 21.0368, "lon": 105.7795},
    {"id": "TL005", "name": "Khu vực Xuân Đỉnh thực",  "lat": 21.0834, "lon": 105.7808},
]

# Khởi tạo tập danh sách xe cố định
VEHICLE_IDS = [f"GPS_{1763886879 + i}" for i in range(50)]


def get_time_context():
    """Hàm nội suy bối cảnh thời gian thực tế."""
    now  = datetime.now()
    dow  = now.weekday()
    hour = now.hour
    day_type    = "weekend" if dow >= 5 else "weekday"
    time_window = "peak_hour" if hour in (6, 7, 8, 16, 17, 18, 19) else "off_peak"
    return day_type, time_window


def delivery_cb(err, msg):
    """Hàm phản hồi trạng thái gửi Kafka."""
    if err:
        print(f"❌ Kafka lỗi: {err}")


print("🏠 Mock Producer → Xả dữ liệu vào luồng LOCAL")
print("Nhấn Ctrl+C để dừng\n")

try:
    while True:
        # Lấy định dạng mốc thời gian chuẩn UTC
        now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        day_type, time_window = get_time_context()

        # Giả lập ping GPS của các phương tiện
        for vid in VEHICLE_IDS:
            tl = random.choice(TRAFFIC_LIGHTS)
            
            # Tỷ lệ 35% phương tiện rơi vào dải ùn tắc
            is_congested = random.random() < 0.35
            speed_ms = random.uniform(0.2, 2.7) if is_congested \
                       else random.uniform(2.8, 13.9)

            # Quy đổi chuẩn xác sang km/h để khớp với GPS_SCHEMA của Data Lake
            speed_kmh = round(speed_ms * 3.6, 2)

            producer.produce(
                "gps_stream_topic",
                key=vid,
                value=json.dumps({
                    "gps_id":           vid,
                    "timestamp":        now_str,
                    "latitude":         tl["lat"] + random.uniform(-0.002, 0.002),
                    "longitude":        tl["lon"] + random.uniform(-0.002, 0.002),
                    "speed_kmh":        speed_kmh,  # <── Khóa chính xác hệ thống mong đợi
                    "traffic_light_id": tl["id"],
                    "day_type":         day_type,
                    "time_window":      time_window,
                }).encode(),
                callback=delivery_cb
            )

            # Xuất log minh bạch ra màn hình nếu xe di chuyển chậm
            if speed_kmh < 10.0:
                print(f"🚗 {vid} đang lết với {speed_kmh} km/h gần {tl['name']} [{time_window}]")

        # Đẩy lô tin sang Kafka
        producer.flush()
        time.sleep(1)

except KeyboardInterrupt:
    print("\n⏹ Dừng Mock Producer an toàn.")
