"""
ingestion_worker.py — Tầng Data Lake
══════════════════════════════════════════════════════════════
Kafka (gps_stream_topic) → batch 100 records → Parquet → MinIO

Schema khớp chính xác data Flutter app:
  gps_id, timestamp, latitude, longitude, speed_kmh,
  traffic_light_id, day_type, time_window, received_at
══════════════════════════════════════════════════════════════
"""
import io
import json
import logging
import sys
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
from botocore.client import Config
from confluent_kafka import Consumer, KafkaError

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Chế độ chạy ───────────────────────────────────────────────
IS_LOCAL = "--local" in sys.argv
ENV_FOLDER = "mock" if IS_LOCAL else "prod"

KAFKA_CONF = {
    "bootstrap.servers": "localhost:9092",
    "group.id":          "ingestion_worker_v1",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}
MINIO_CONF = {
    "endpoint_url":          "http://localhost:9000",
    "aws_access_key_id":     "tk", #tài khoản đã đăng ký
    "aws_secret_access_key": "password", #mật khẩu đăng ký tài khoản
    "config":                Config(signature_version="s3v4"),
    "region_name":           "us-east-1",
}
BUCKET     = "traffic-lake"
BATCH_SIZE = 100
TOPIC      = "gps_stream_topic"

# Schema Parquet — khớp chính xác với data Flutter app sau khi app.py convert
GPS_SCHEMA = pa.schema([
    pa.field("gps_id",           pa.string()),
    pa.field("timestamp",        pa.string()),    # "2025-11-23 15:34:39"
    pa.field("latitude",         pa.float64()),
    pa.field("longitude",        pa.float64()),
    pa.field("speed_kmh",        pa.float64()),   # đã convert từ m/s trong app.py
    pa.field("traffic_light_id", pa.string()),    # "TL005" hoặc "unknown"
    pa.field("day_type",         pa.string()),    # "weekday" | "weekend"
    pa.field("time_window",      pa.string()),    # "peak_hour" | "off_peak"
    pa.field("received_at",      pa.string()),
])


def get_s3():
    return boto3.client("s3", **MINIO_CONF)


def ensure_bucket(s3):
    try:
        s3.head_bucket(Bucket=BUCKET)
    except Exception:
        s3.create_bucket(Bucket=BUCKET)
        log.info(f"✅ Tạo bucket: {BUCKET}")


def flush(s3, batch: list) -> bool:
    """Ghi batch records thành 1 file Parquet lên MinIO."""
    if not batch:
        return True

    now = datetime.now(timezone.utc)

    # Đảm bảo không có None trong các field string
    for r in batch:
        r.setdefault("traffic_light_id", "unknown")
        r.setdefault("day_type",         "weekday")
        r.setdefault("time_window",      "off_peak")
        r.setdefault("received_at",      now.isoformat())

    df    = pd.DataFrame(batch)
    # Chỉ giữ các cột theo schema, loại bỏ extra fields
    for col in GPS_SCHEMA.names:
        if col not in df.columns:
            df[col] = None
    df = df[GPS_SCHEMA.names]

    table = pa.Table.from_pandas(df, schema=GPS_SCHEMA, preserve_index=False)
    buf   = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    # Phân vùng theo thư mục môi trường và ngày
    key = (f"{ENV_FOLDER}/gps/"
           f"year={now.year}/month={now.month:02d}/day={now.day:02d}/"
           f"gps_{now.strftime('%H%M%S_%f')}.parquet")

    try:
        s3.put_object(Bucket=BUCKET, Key=key, Body=buf.getvalue())
        log.info(f"📦 Flushed {len(batch)} records → s3://{BUCKET}/{key}")
        return True
    except Exception as e:
        log.error(f"❌ MinIO upload lỗi: {e}")
        return False


def main():
    s3 = get_s3()
    ensure_bucket(s3)

    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe([TOPIC])
    
    env_label = "LOCAL (Mock data)" if IS_LOCAL else "CLOUD (Production data)"
    log.info(f"🎧 Ingestion Worker [{env_label}] lắng nghe [{TOPIC}]... (Ctrl+C để dừng)")

    buffer = []

    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                # Poll timeout — flush nếu có data đang chờ
                if buffer:
                    if flush(s3, buffer.copy()):
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
                if len(buffer) >= BATCH_SIZE:
                    if flush(s3, buffer.copy()):
                        consumer.commit()
                        buffer.clear()
            except json.JSONDecodeError as e:
                log.warning(f"⚠️ JSON parse lỗi: {e}")
            except Exception as e:
                log.error(f"❌ Lỗi xử lý message: {e}")

    except KeyboardInterrupt:
        log.info("\n🛑 Dừng — flush dữ liệu còn lại...")
        if buffer:
            flush(s3, buffer)
    finally:
        consumer.close()
        log.info("✅ Ingestion Worker đã đóng.")


if __name__ == "__main__":
    main()
