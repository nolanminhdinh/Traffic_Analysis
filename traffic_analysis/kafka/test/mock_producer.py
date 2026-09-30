"""
mock_producer.py — Sinh dữ liệu giao thông giả lập phục vụ kiểm thử hệ thống
═══════════════════════════════════════════════════════════════════════════════
Chức năng:
  Mô phỏng hành vi di chuyển thực tế của các đoàn phương tiện qua 5 nút giao thông
  trên trục đường Phạm Văn Đồng, phục vụ kiểm thử tự động toàn diện:
    mock_producer -> Kafka -> ingestion_worker (--local) -> MinIO -> etl_worker (--local) -> PostgreSQL Local

Các thông số giả lập:
  - Tỷ lệ ùn tắc ngẫu nhiên (mặc định 35% xe di chuyển chậm 0.2 - 2.7 m/s ~ 0.7 - 9.7 km/h).
  - Tự động suy luận bối cảnh thời gian: weekday / weekend, peak_hour / off_peak.
  - Hỗ trợ tham số dòng lệnh tùy chỉnh số xe, chu kỳ phát tin.
═══════════════════════════════════════════════════════════════════════════════
"""

import argparse
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv

# Load biến môi trường từ .env
env_path = Path(__file__).resolve().parent.parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("MockProducer")

# Cấu hình Kafka từ biến môi trường
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GPS", "gps_stream_topic")

# Danh mục nút giao thông trên tuyến Phạm Văn Đồng khớp với bảng intersections trong DB
TRAFFIC_LIGHTS = [
    {"id": "TL001", "name": "Ngã tư Xuân Đỉnh",      "lat": 21.0664, "lon": 105.7815},
    {"id": "TL002", "name": "Ngã tư Cổ Nhuế",        "lat": 21.0562, "lon": 105.7823},
    {"id": "TL003", "name": "Ngã tư Hoàng Quốc Việt","lat": 21.0458, "lon": 105.7831},
    {"id": "TL004", "name": "Ngã tư Mai Dịch",        "lat": 21.0368, "lon": 105.7795},
    {"id": "TL005", "name": "Khu vực Xuân Đỉnh thực","lat": 21.0834, "lon": 105.7808},
]


def get_current_time_context():
    """Xác định ngữ cảnh thời gian thực tế."""
    now = datetime.now()
    day_type = "weekend" if now.weekday() >= 5 else "weekday"
    time_window = "peak_hour" if now.hour in (6, 7, 8, 16, 17, 18, 19) else "off_peak"
    return day_type, time_window


def delivery_report(err, msg):
    """Callback nhận kết quả gửi Kafka."""
    if err:
        log.error(f"❌ Lỗi gửi tin Kafka: {err}")


def main():
    parser = argparse.ArgumentParser(description="Sinh dữ liệu GPS phương tiện giả lập.")
    parser.add_argument("--vehicles", type=int, default=50, help="Số lượng xe mô phỏng (mặc định: 50)")
    parser.add_argument("--interval", type=float, default=1.0, help="Khoảng cách giữa các chu kỳ gửi (giây, mặc định: 1.0)")
    parser.add_argument("--congested-ratio", type=float, default=0.35, help="Tỷ lệ xe gặp tắc đường (0.0 đến 1.0, mặc định: 0.35)")
    args = parser.parse_args()

    vehicle_ids = [f"GPS_{1763886879 + i}" for i in range(args.vehicles)]

    log.info("=" * 65)
    log.info("🚗 Khởi động Mock Producer (Kiểm thử dòng dữ liệu)")
    log.info(f"📍 Kafka Broker: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC}")
    log.info(f"📍 Số phương tiện: {args.vehicles} | Tần suất: {args.interval}s/lượt")
    log.info(f"📍 Tỷ lệ ùn tắc: {int(args.congested_ratio * 100)}%")
    log.info("=" * 65)

    try:
        producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "client.id": "mock-traffic-producer",
            "retries": 3,
        })
    except Exception as e:
        log.error(f"❌ Không thể tạo Kafka Producer: {e}")
        log.info("💡 Gợi ý: Hãy chắc chắn Docker đang chạy: `docker compose up -d`")
        sys.exit(1)

    try:
        while True:
            now_utc_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            day_type, time_window = get_current_time_context()

            congested_count = 0

            for vid in vehicle_ids:
                tl = random.choice(TRAFFIC_LIGHTS)
                is_congested = random.random() < args.congested_ratio

                if is_congested:
                    # Xe di chuyển chậm / lết (0.2 m/s -> 2.7 m/s ~ 0.72 - 9.72 km/h)
                    speed_ms = random.uniform(0.2, 2.7)
                    congested_count += 1
                else:
                    # Xe lưu thông bình thường (2.8 m/s -> 13.9 m/s ~ 10 - 50 km/h)
                    speed_ms = random.uniform(2.8, 13.9)

                speed_kmh = round(speed_ms * 3.6, 2)

                payload = {
                    "gps_id": vid,
                    "timestamp": now_utc_str,
                    "latitude": tl["lat"] + random.uniform(-0.0015, 0.0015),
                    "longitude": tl["lon"] + random.uniform(-0.0015, 0.0015),
                    "speed_kmh": speed_kmh,
                    "traffic_light_id": tl["id"],
                    "day_type": day_type,
                    "time_window": time_window,
                }

                try:
                    producer.produce(
                        KAFKA_TOPIC,
                        key=vid,
                        value=json.dumps(payload).encode("utf-8"),
                        callback=delivery_report
                    )
                except KafkaException as ke:
                    log.error(f"Kafka produce error: {ke}")
                    break

            producer.poll(0)
            producer.flush(timeout=2.0)

            log.info(
                f"📡 Đã đẩy {len(vehicle_ids)} pings GPS | "
                f"Tắc nghẽn: {congested_count}/{len(vehicle_ids)} xe | "
                f"[{day_type}, {time_window}]"
            )

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("\n⏹ Dừng Mock Producer an toàn...")
    finally:
        producer.flush(timeout=5.0)
        log.info("✅ Mock Producer đã kết thúc.")


if __name__ == "__main__":
    main()
