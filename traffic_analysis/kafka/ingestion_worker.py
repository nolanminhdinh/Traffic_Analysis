"""
ingestion_worker.py — Tầng Data Lake (Kafka -> Parquet -> MinIO Object Storage)
═══════════════════════════════════════════════════════════════════════════════
Chức năng:
  1. Lắng nghe dữ liệu stream từ Apache Kafka topic (gps_stream_topic).
  2. Gom cụm micro-batch (mặc định 100 bản ghi hoặc sau khoảng thời gian rỗi).
  3. Chuẩn hóa về columnar format Apache Parquet với schema cố định (PyArrow).
  4. Đẩy file Parquet đã nén Snappy lên MinIO S3 Object Storage.
  5. Phân vùng đường dẫn lưu trữ:
     - Chế độ Production: prod/gps/year=.../month=.../day=.../gps_*.parquet
     - Chế độ Local/Mock: mock/gps/year=.../month=.../day=.../gps_*.parquet

Cách chạy:
  python ingestion_worker.py          # Xử lý dữ liệu thật (Production)
  python ingestion_worker.py --local  # Xử lý dữ liệu kiểm thử giả lập (Mock)
═══════════════════════════════════════════════════════════════════════════════
"""

import io
import json
import logging
import os
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# Load biến môi trường từ .env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("IngestionWorker")

# Xác định chế độ môi trường
IS_LOCAL = "--local" in sys.argv
ENV_FOLDER = "mock" if IS_LOCAL else "prod"

# Cấu hình Kafka & MinIO từ biến môi trường
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP = os.getenv("KAFKA_GROUP_ID", "ingestion_worker_v1")
if IS_LOCAL:
    KAFKA_GROUP = f"{KAFKA_GROUP}_local"

KAFKA_TOPIC = os.getenv("KAFKA_TOPIC_GPS", "gps_stream_topic")
BATCH_SIZE = int(os.getenv("INGESTION_BATCH_SIZE", 100))

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "admin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

KAFKA_CONF = {
    "bootstrap.servers": KAFKA_BOOTSTRAP,
    "group.id": KAFKA_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

MINIO_CONF = {
    "endpoint_url": MINIO_ENDPOINT,
    "aws_access_key_id": MINIO_ACCESS_KEY,
    "aws_secret_access_key": MINIO_SECRET_KEY,
    "config": Config(signature_version="s3v4"),
    "region_name": MINIO_REGION,
}

# Định nghĩa Schema chuẩn hóa cho Parquet Data Lake
GPS_SCHEMA = pa.schema([
    pa.field("gps_id", pa.string()),
    pa.field("timestamp", pa.string()),
    pa.field("latitude", pa.float64()),
    pa.field("longitude", pa.float64()),
    pa.field("speed_kmh", pa.float64()),
    pa.field("traffic_light_id", pa.string()),
    pa.field("day_type", pa.string()),
    pa.field("time_window", pa.string()),
    pa.field("received_at", pa.string()),
])


def get_s3_client():
    """Khởi tạo S3 client kết nối MinIO."""
    return boto3.client("s3", **MINIO_CONF)


def ensure_bucket_exists(s3):
    """Kiểm tra và tự động khởi tạo bucket nếu chưa có."""
    try:
        s3.head_bucket(Bucket=MINIO_BUCKET)
        log.info(f"✅ Đã kết nối MinIO bucket: '{MINIO_BUCKET}'")
    except ClientError:
        try:
            s3.create_bucket(Bucket=MINIO_BUCKET)
            log.info(f"✨ Đã tạo mới MinIO bucket: '{MINIO_BUCKET}'")
        except Exception as e:
            log.error(f"❌ Không thể tạo bucket '{MINIO_BUCKET}': {e}")
            raise


def flush_batch(s3, batch: list) -> bool:
    """Chuyển đổi danh sách bản ghi thành Parquet và đẩy lên MinIO."""
    if not batch:
        return True

    now = datetime.now(timezone.utc)

    # Đảm bảo các trường mặc định không bị thiếu
    for record in batch:
        record.setdefault("traffic_light_id", "unknown")
        record.setdefault("day_type", "weekday")
        record.setdefault("time_window", "off_peak")
        record.setdefault("received_at", now.isoformat())

    # Chuyển đổi thành DataFrame và lọc đúng schema
    df = pd.DataFrame(batch)
    for col in GPS_SCHEMA.names:
        if col not in df.columns:
            df[col] = None
    df = df[GPS_SCHEMA.names]

    # Ghi định dạng Parquet với chuẩn nén Snappy
    table = pa.Table.from_pandas(df, schema=GPS_SCHEMA, preserve_index=False)
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    buffer.seek(0)

    # Đường dẫn phân vùng thời gian: year/month/day
    s3_key = (
        f"{ENV_FOLDER}/gps/"
        f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"gps_{now.strftime('%H%M%S_%f')}.parquet"
    )

    try:
        s3.put_object(Bucket=MINIO_BUCKET, Key=s3_key, Body=buffer.getvalue())
        log.info(f"📦 Flushed {len(batch)} bản ghi -> s3://{MINIO_BUCKET}/{s3_key}")
        return True
    except Exception as e:
        log.error(f"❌ Lỗi tải file Parquet lên MinIO: {e}")
        return False


def main():
    mode_str = "MOCK / LOCAL TESTING" if IS_LOCAL else "PRODUCTION / REAL DATA"
    log.info("=" * 65)
    log.info(f"🎧 Khởi động Ingestion Worker [{mode_str}]")
    log.info(f"📍 Kafka: {KAFKA_BOOTSTRAP} | Topic: {KAFKA_TOPIC} | Group: {KAFKA_GROUP}")
    log.info(f"📍 MinIO Endpoint: {MINIO_ENDPOINT} | Bucket: {MINIO_BUCKET} | Thư mục: {ENV_FOLDER}/")
    log.info("=" * 65)

    s3 = get_s3_client()
    try:
        ensure_bucket_exists(s3)
    except Exception as e:
        log.error(f"MinIO không khả dụng. Kiểm tra docker compose. Chi tiết: {e}")
        sys.exit(1)

    try:
        consumer = Consumer(KAFKA_CONF)
        consumer.subscribe([KAFKA_TOPIC])
    except Exception as e:
        log.error(f"Không thể kết nối tới Kafka: {e}")
        sys.exit(1)

    buffer = []
    running = True

    def handle_shutdown(signum, frame):
        nonlocal running
        log.info("\n🛑 Nhận tín hiệu dừng, tiến hành dọn dẹp và flush buffer...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        while running:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                # Không có tin mới trong 1 giây -> Nếu buffer còn dữ liệu thì flush ngay
                if buffer:
                    if flush_batch(s3, buffer.copy()):
                        consumer.commit()
                        buffer.clear()
                continue

            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error(f"Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode("utf-8"))
                buffer.append(data)

                # Khi buffer đạt giới hạn batch_size -> flush lên MinIO
                if len(buffer) >= BATCH_SIZE:
                    if flush_batch(s3, buffer.copy()):
                        consumer.commit()
                        buffer.clear()
            except json.JSONDecodeError as e:
                log.warning(f"⚠️ Không thể giải mã JSON từ message: {e}")
            except Exception as e:
                log.error(f"❌ Lỗi xử lý message: {e}")

    finally:
        # Flush toàn bộ dữ liệu còn sót lại trong buffer trước khi tắt
        if buffer:
            log.info(f"Xử lý {len(buffer)} bản ghi còn lại trước khi ngắt...")
            flush_batch(s3, buffer)
            consumer.commit()
        consumer.close()
        log.info("✅ Ingestion Worker đã dừng an toàn.")


if __name__ == "__main__":
    main()
