"""
etl_worker.py — Tầng Data Warehouse & Suy luận ùn tắc giao thông
═══════════════════════════════════════════════════════════════════════════════
Chức năng:
  1. Quét định kỳ các tệp Parquet mới sinh từ MinIO Data Lake.
  2. ETL Step 1 (Data Cleaning & Loading):
     - Lọc dữ liệu GPS hợp lệ (-90 <= lat <= 90, -180 <= lon <= 180, 0 <= speed <= 200).
     - Loại bỏ tọa độ nhiễu (gần 0, 0) và bản ghi trùng lặp (gps_id, timestamp).
     - Bulk insert vào bảng phân vùng vehicle_gps trong PostgreSQL / TimescaleDB.
  3. ETL Step 2 (Traffic Inference Analysis):
     - Phân tích cửa sổ trượt 5 phút gần nhất tại từng nút giao thông.
     - Tính toán vận tốc trung bình (avg_speed_kmh), số lượng xe (vehicle_count).
     - Tính mức độ ùn tắc (congestion_level: 0 - 100), gán nhãn (congestion_label).
     - Ước tính thời gian chờ (avg_waiting_time) và chiều dài hàng đợi (queue_length).
     - Đánh giá tính phi hiệu quả của chu kỳ đèn tĩnh (is_inefficient):
       Nếu có >= 3 xe với vận tốc < 5 km/h -> chu kỳ đèn hiện tại không đáp ứng.
     - Lưu kết quả vào bảng congestion_analysis.

Cách chạy:
  python etl_worker.py          # Ghi vào NEON CLOUD (Dữ liệu thực tế từ App)
  python etl_worker.py --local  # Ghi vào LOCAL DOCKER (Dữ liệu giả lập Mock)
═══════════════════════════════════════════════════════════════════════════════
"""

import io
import logging
import os
import signal
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3
from botocore.client import Config
from dotenv import load_dotenv
import pandas as pd
import psycopg2
import psycopg2.extras
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
log = logging.getLogger("ETLWorker")

# Kiểm tra cờ chạy môi trường
IS_LOCAL = "--local" in sys.argv

# Cấu hình Database theo môi trường
if IS_LOCAL:
    DB_CONFIG = {
        "host":     os.getenv("LOCAL_DB_HOST", "localhost"),
        "port":     int(os.getenv("LOCAL_DB_PORT", 5433)),
        "database": os.getenv("LOCAL_DB_NAME", "traffic_analytics"),
        "user":     os.getenv("LOCAL_DB_USER", "admin"),
        "password": os.getenv("LOCAL_DB_PASSWORD", "admin123"),
    }
    ENV_LABEL = "LOCAL DOCKER (TimescaleDB / Postgres)"
    DATA_PREFIX = "mock/gps/"
else:
    DB_CONFIG = {
        "host":            os.getenv("CLOUD_DB_HOST", "ep-old-voice-a1mw52bq-pooler.ap-southeast-1.aws.neon.tech"),
        "port":            int(os.getenv("CLOUD_DB_PORT", 5432)),
        "database":        os.getenv("CLOUD_DB_NAME", "neondb"),
        "user":            os.getenv("CLOUD_DB_USER", "neondb_owner"),
        "password":        os.getenv("CLOUD_DB_PASSWORD", ""),
        "sslmode":         os.getenv("CLOUD_DB_SSLMODE", "require"),
        "options":         "-c statement_timeout=30000",
        "keepalives":      1,
        "keepalives_idle": 30,
    }
    ENV_LABEL = "CLOUD NEON (Serverless Postgres)"
    DATA_PREFIX = "prod/gps/"

# Cấu hình MinIO
MINIO_CONFIG = {
    "endpoint_url":          os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    "aws_access_key_id":     os.getenv("MINIO_ACCESS_KEY", "admin"),
    "aws_secret_access_key": os.getenv("MINIO_SECRET_KEY", "admin123"),
    "config":                Config(signature_version="s3v4"),
    "region_name":           os.getenv("MINIO_REGION", "us-east-1"),
}
BUCKET = os.getenv("MINIO_BUCKET", "traffic-lake")

# Tham số phân tích giao thông
ETL_INTERVAL_SEC             = int(os.getenv("ETL_INTERVAL_SEC", 60))
SPEED_CONGESTED_KMH          = float(os.getenv("SPEED_CONGESTED_KMH", 10.0))
SPEED_SLOW_KMH               = float(os.getenv("SPEED_SLOW_KMH", 25.0))
INEFFICIENT_SPEED_THRESHOLD  = float(os.getenv("INEFFICIENT_SPEED_THRESHOLD", 5.0))
INEFFICIENT_VEHICLE_MIN_COUNT = int(os.getenv("INEFFICIENT_VEHICLE_MIN_COUNT", 3))


def get_s3_client():
    """Khởi tạo S3 client kết nối MinIO."""
    return boto3.client("s3", **MINIO_CONFIG)


def get_db_connection():
    """Khởi tạo kết nối PostgreSQL với autocommit = False."""
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def list_new_parquet_files(s3, prefix: str, since: datetime) -> list:
    """Liệt kê các tệp Parquet mới được tạo trên MinIO sau thời điểm `since`."""
    files = []
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
            for obj in page.get("Contents", []):
                last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
                if last_modified > since:
                    files.append(obj["Key"])
    except Exception as e:
        log.warning(f"Lỗi khi quét file từ MinIO prefix '{prefix}': {e}")
    return sorted(files)


def read_parquet_from_s3(s3, key: str) -> pd.DataFrame:
    """Đọc tệp Parquet trực tiếp từ MinIO vào Pandas DataFrame."""
    response = s3.get_object(Bucket=BUCKET, Key=key)
    buf = io.BytesIO(response["Body"].read())
    return pq.read_table(buf).to_pandas()


def etl_load_gps(s3, conn, since: datetime) -> int:
    """
    Bước 1: Đọc Parquet từ MinIO, làm sạch và nạp vào bảng vehicle_gps.
    """
    new_files = list_new_parquet_files(s3, DATA_PREFIX, since)
    if not new_files:
        return 0

    log.info(f"📂 Tìm thấy {len(new_files)} tệp Parquet mới tại '{DATA_PREFIX}'")
    dfs = []
    for f in new_files:
        try:
            dfs.append(read_parquet_from_s3(s3, f))
        except Exception as e:
            log.error(f"❌ Không thể đọc tệp {f}: {e}")

    if not dfs:
        return 0

    df = pd.concat(dfs, ignore_index=True)

    # Làm sạch dữ liệu
    df = df.dropna(subset=["gps_id", "timestamp", "latitude", "longitude"])
    df = df[df["latitude"].between(-90.0, 90.0)]
    df = df[df["longitude"].between(-180.0, 180.0)]
    df = df[df["speed_kmh"].between(0.0, 200.0)]
    # Lọc bỏ tọa độ rác gần (0, 0)
    df = df[~((df["latitude"].abs() < 0.001) & (df["longitude"].abs() < 0.001))]
    # Loại bỏ bản ghi trùng lặp
    df = df.drop_duplicates(subset=["gps_id", "timestamp"])

    if df.empty:
        log.info("  Tất cả bản ghi sau khi làm sạch đều rỗng.")
        return 0

    cur = conn.cursor()
    insert_sql = """
        INSERT INTO vehicle_gps (
            gps_id, recorded_at, latitude, longitude, speed_kmh,
            traffic_light_id, day_type, time_window
        ) VALUES %s
        ON CONFLICT (gps_id, recorded_at) DO NOTHING;
    """
    records = [
        (
            str(row.gps_id),
            row.timestamp,
            float(row.latitude),
            float(row.longitude),
            float(row.speed_kmh),
            str(row.traffic_light_id) if pd.notna(row.traffic_light_id) and row.traffic_light_id != "unknown" else None,
            str(row.day_type) if pd.notna(row.day_type) else None,
            str(row.time_window) if pd.notna(row.time_window) else None,
        )
        for row in df.itertuples(index=False)
    ]

    psycopg2.extras.execute_values(cur, insert_sql, records, page_size=1000)
    inserted_count = cur.rowcount
    conn.commit()
    cur.close()

    log.info(f"✅ Đã nạp thành công {inserted_count}/{len(df)} bản ghi GPS vào vehicle_gps")
    return len(df)


def etl_run_analysis(conn) -> int:
    """
    Bước 2: Phân tích lưu lượng và suy luận tình trạng tắc đường tại các nút giao.
    """
    cur = conn.cursor()
    analysis_sql = """
        INSERT INTO congestion_analysis (
            traffic_light_id, analysis_time, time_id,
            time_window, day_type,
            avg_speed_kmh, vehicle_count,
            congestion_level, congestion_label,
            avg_waiting_time, queue_length,
            scheduled_red_duration,
            is_inefficient
        )
        WITH
        -- 1. Thống kê phương tiện trong cửa sổ 5 phút gần nhất quanh các cột đèn
        recent AS (
            SELECT
                traffic_light_id,
                time_window,
                day_type,
                AVG(speed_kmh)             AS avg_speed,
                COUNT(DISTINCT gps_id)     AS veh_count
            FROM vehicle_gps
            WHERE recorded_at > NOW() - INTERVAL '5 minutes'
              AND traffic_light_id IS NOT NULL
            GROUP BY traffic_light_id, time_window, day_type
        ),
        -- 2. Tra cứu thời lượng chu kỳ đèn tĩnh tương ứng với khung giờ hiện tại
        sched AS (
            SELECT traffic_light_id, red_duration, applicable_days
            FROM light_schedule
            WHERE CURRENT_TIME BETWEEN start_time AND end_time
        ),
        -- 3. Khớp mã chiều thời gian (time_dimension)
        cur_time AS (
            SELECT time_id
            FROM time_dimension
            WHERE date_val = CURRENT_DATE
              AND hour = EXTRACT(HOUR FROM NOW())::SMALLINT
            LIMIT 1
        )
        SELECT
            r.traffic_light_id,
            NOW()                                                AS analysis_time,
            ct.time_id,
            r.time_window,
            r.day_type,
            ROUND(r.avg_speed::numeric, 2)                       AS avg_speed_kmh,
            r.veh_count                                          AS vehicle_count,

            -- Tính chỉ số ùn tắc trong thang điểm 0 - 100
            ROUND(GREATEST(0, LEAST(100,
                CASE
                    WHEN r.avg_speed <= 0 THEN 100
                    WHEN r.avg_speed >= 50 THEN 0
                    ELSE (1 - r.avg_speed / 50.0) * 100
                END
            ))::numeric, 1)                                      AS congestion_level,

            -- Gán nhãn phân loại giao thông
            CASE
                WHEN r.avg_speed < %(cong_speed)s THEN 'Tắc nghẽn'
                WHEN r.avg_speed < %(slow_speed)s THEN 'Chậm'
                ELSE 'Thông thoáng'
            END                                                  AS congestion_label,

            -- Ước tính thời gian chờ trung bình (giây)
            CASE
                WHEN r.avg_speed < 1.0               THEN 120
                WHEN r.avg_speed < %(cong_speed)s    THEN 60
                WHEN r.avg_speed < %(slow_speed)s    THEN 20
                ELSE 5
            END::DOUBLE PRECISION                                AS avg_waiting_time,

            -- Ước tính chiều dài hàng đợi xe
            GREATEST(0, (%(cong_speed)s - r.avg_speed) / NULLIF(%(cong_speed)s, 0.0) * r.veh_count)
                                                                 AS queue_length,

            s.red_duration                                       AS scheduled_red_duration,

            -- Suy luận chu kỳ đèn kém hiệu quả (is_inefficient)
            CASE
                WHEN r.veh_count >= %(min_vehicles)s AND r.avg_speed < %(inefficient_speed)s
                THEN TRUE
                ELSE FALSE
            END                                                  AS is_inefficient

        FROM recent r
        LEFT JOIN sched s
            ON s.traffic_light_id = r.traffic_light_id
            AND s.applicable_days = r.day_type
        LEFT JOIN cur_time ct ON TRUE

        ON CONFLICT (traffic_light_id, analysis_time) DO NOTHING;
    """

    params = {
        "cong_speed": SPEED_CONGESTED_KMH,
        "slow_speed": SPEED_SLOW_KMH,
        "min_vehicles": INEFFICIENT_VEHICLE_MIN_COUNT,
        "inefficient_speed": INEFFICIENT_SPEED_THRESHOLD
    }

    cur.execute(analysis_sql, params)
    analyzed_count = cur.rowcount
    conn.commit()
    cur.close()

    if analyzed_count > 0:
        log.info(f"📊 Đã phân tích và lưu {analyzed_count} bản ghi đánh giá ùn tắc -> congestion_analysis")
    return analyzed_count


def main():
    log.info("=" * 65)
    log.info(f"🔄 Khởi động ETL Worker | Môi trường mục tiêu: {ENV_LABEL}")
    log.info(f"📍 Database: {DB_CONFIG.get('host')}:{DB_CONFIG.get('port')}/{DB_CONFIG.get('database')}")
    log.info(f"📍 Data Lake Source: s3://{BUCKET}/{DATA_PREFIX}")
    log.info(f"📍 Chu kỳ chạy: {ETL_INTERVAL_SEC} giây")
    log.info("=" * 65)

    s3 = get_s3_client()
    conn = None
    last_run_time = datetime.now(timezone.utc) - timedelta(hours=2)
    running = True

    def handle_shutdown(signum, frame):
        nonlocal running
        log.info("\n🛑 Nhận tín hiệu dừng ETL Worker...")
        running = False

    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    try:
        conn = get_db_connection()
        log.info("✅ Kết nối Database thành công!")
    except Exception as e:
        log.error(f"❌ Không thể kết nối Database ({ENV_LABEL}): {e}")
        sys.exit(1)

    try:
        while running:
            iteration_start = datetime.now(timezone.utc)
            log.info(f"\n--- [ETL Cycle: {iteration_start.strftime('%Y-%m-%d %H:%M:%S UTC')}] ---")

            try:
                # 1. Nạp GPS
                gps_count = etl_load_gps(s3, conn, last_run_time)

                # 2. Phân tích ùn tắc
                etl_run_analysis(conn)

                # 3. Dọn dẹp dữ liệu cũ nếu chạy local
                if IS_LOCAL:
                    clean_cur = conn.cursor()
                    clean_cur.execute("DELETE FROM vehicle_gps WHERE recorded_at < NOW() - INTERVAL '7 days';")
                    conn.commit()
                    clean_cur.close()

                if gps_count == 0:
                    log.info("ℹ️  Không phát sinh dữ liệu GPS mới cần nạp.")

            except psycopg2.OperationalError as oe:
                log.warning(f"⚠️ Mất kết nối DB ({oe}) - Đang thử kết nối lại...")
                try:
                    if conn:
                        conn.close()
                except Exception:
                    pass
                try:
                    time.sleep(3)
                    conn = get_db_connection()
                    log.info("🔄 Kết nối lại DB thành công!")
                except Exception as reconnect_err:
                    log.error(f"❌ Kết nối lại DB thất bại: {reconnect_err}")

            except Exception as e:
                log.error(f"❌ Lỗi thực thi ETL: {e}", exc_info=True)
                if conn:
                    try:
                        conn.rollback()
                    except Exception:
                        pass

            last_run_time = iteration_start

            # Đợi đến chu kỳ tiếp theo
            elapsed = (datetime.now(timezone.utc) - iteration_start).total_seconds()
            sleep_duration = max(1.0, ETL_INTERVAL_SEC - elapsed)
            log.info(f"💤 Nghỉ {sleep_duration:.1f}s trước chu kỳ kế tiếp...")

            # Cho phép ngắt vòng lặp mượt mà
            wait_step = 1.0
            total_slept = 0.0
            while running and total_slept < sleep_duration:
                time.sleep(min(wait_step, sleep_duration - total_slept))
                total_slept += wait_step

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass
        log.info("✅ ETL Worker đã đóng an toàn.")


if __name__ == "__main__":
    main()
