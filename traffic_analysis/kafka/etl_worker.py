"""
etl_worker.py — Tầng Warehouse (Phiên bản Tối ưu - Không Streaming Đèn)
══════════════════════════════════════════════════════════════
MinIO (Parquet GPS) → PostgreSQL

Chạy với 1 trong 2 chế độ:
  python etl_worker.py         → CLOUD  (Neon, nhận data thật từ app)
  python etl_worker.py --local → LOCAL  (Docker, nhận data mock)
══════════════════════════════════════════════════════════════
"""
import io
import logging
import sys
import time
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
from botocore.client import Config

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Chế độ chạy ───────────────────────────────────────────────
IS_LOCAL = "--local" in sys.argv

# ── Cấu hình DB theo môi trường ───────────────────────────────
if IS_LOCAL:
    DB_CONFIG = {
        "host":     "localhost",
        "port":     5433,
        "database": "traffic_analytics",
        "user":     "admin",
        "password": "admin123",
    }
    ENV_LABEL = "LOCAL"
else:
    DB_CONFIG = {
        "host":            "****",
        "port":            5432,
        "database":        "neondb",
        "user":            "neondb_owner",
        "password":        "*****",
        "sslmode":         "require",
        "options":         "-c statement_timeout=30000",
        "keepalives":      1,
        "keepalives_idle": 30,
    }
    ENV_LABEL = "☁️  CLOUD (Neon)"

ETL_INTERVAL_SEC    = 60
SPEED_CONGESTED_KMH = 10.0
SPEED_SLOW_KMH      = 25.0

MINIO_CONFIG = {
    "endpoint_url":          "http://localhost:9000",
    "aws_access_key_id":     "taikhoan", #tài khoản đã đăng ký
    "aws_secret_access_key": "password", #mẩt khẩu đăng ky đăng nhập tài khoản
    "config":                Config(signature_version="s3v4"),
    "region_name":           "us-east-1",
}
BUCKET = "traffic-lake"


def get_s3():
    return boto3.client("s3", **MINIO_CONFIG)


def get_db():
    conn = psycopg2.connect(**DB_CONFIG)
    conn.autocommit = False
    return conn


def list_new_files(s3, prefix: str, since: datetime) -> list:
    files, pager = [], s3.get_paginator("list_objects_v2")
    for page in pager.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["LastModified"].replace(tzinfo=timezone.utc) > since:
                files.append(obj["Key"])
    return sorted(files)


def read_parquet(s3, key: str) -> pd.DataFrame:
    buf = io.BytesIO(s3.get_object(Bucket=BUCKET, Key=key)["Body"].read())
    return pq.read_table(buf).to_pandas()


# ══════════════════════════════════════════════════════════════
# ETL Step 1: Parquet GPS → vehicle_gps
# ══════════════════════════════════════════════════════════════
def etl_gps(s3, conn, since: datetime) -> int:
    prefix = "mock/gps/" if IS_LOCAL else "prod/gps/"
    files = list_new_files(s3, prefix, since)
    if not files:
        return 0

    df = pd.concat([read_parquet(s3, f) for f in files], ignore_index=True)

    # Làm sạch
    df = df.dropna(subset=["gps_id", "timestamp", "latitude", "longitude"])
    df = df[df["latitude"].between(-90, 90)]
    df = df[df["longitude"].between(-180, 180)]
    df = df[df["speed_kmh"].between(0, 200)]
    df = df[~((df["latitude"].abs() < 0.001) & (df["longitude"].abs() < 0.001))]
    df = df.drop_duplicates(subset=["gps_id", "timestamp"])

    if df.empty:
        return 0

    cur = conn.cursor()
    psycopg2.extras.execute_values(cur, """
        INSERT INTO vehicle_gps
            (gps_id, recorded_at, latitude, longitude, speed_kmh,
             traffic_light_id, day_type, time_window)
        VALUES %s
        ON CONFLICT (gps_id, recorded_at) DO NOTHING
    """, [
        (
            str(row.gps_id),
            row.timestamp,
            float(row.latitude),
            float(row.longitude),
            float(row.speed_kmh),
            str(row.traffic_light_id) if pd.notna(row.traffic_light_id) and row.traffic_light_id != "unknown" else None,
            str(row.day_type)    if pd.notna(row.day_type)    else None,
            str(row.time_window) if pd.notna(row.time_window) else None,
        )
        for row in df.itertuples(index=False)
    ])

    conn.commit()
    cur.close()
    log.info(f"  GPS: {len(df)} bản ghi từ {len(files)} file(s)")
    return len(df)


# ══════════════════════════════════════════════════════════════
# ETL Step 2: Tính congestion_analysis (Dựa trên dữ liệu tĩnh)
# ══════════════════════════════════════════════════════════════
def etl_analysis(conn) -> int:
    cur = conn.cursor()
    cur.execute("""
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
        -- 1. Thống kê dòng xe 5 phút qua tại các nút giao
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
        -- 2. Lấy thông số thời lượng đèn đỏ tĩnh theo khung giờ hiện tại
        sched AS (
            SELECT traffic_light_id, red_duration, applicable_days
            FROM light_schedule
            WHERE CURRENT_TIME BETWEEN start_time AND end_time
        ),
        -- 3. Tra cứu time_id
        cur_time AS (
            SELECT time_id
            FROM time_dimension
            WHERE date_val = CURRENT_DATE
              AND hour = EXTRACT(HOUR FROM NOW())::SMALLINT
            LIMIT 1
        )
        SELECT
            r.traffic_light_id,
            NOW()                                        AS analysis_time,
            ct.time_id,
            r.time_window,
            r.day_type,
            ROUND(r.avg_speed::numeric, 2)               AS avg_speed_kmh,
            r.veh_count                                  AS vehicle_count,
            
            -- Tính toán dải ùn tắc 0-100
            ROUND(GREATEST(0, LEAST(100,
                CASE
                    WHEN r.avg_speed <= 0  THEN 100
                    WHEN r.avg_speed >= 50 THEN 0
                    ELSE (1 - r.avg_speed / 50.0) * 100
                END
            ))::numeric, 1)                              AS congestion_level,
            
            -- Gán nhãn trực quan
            CASE
                WHEN r.avg_speed < %(cong)s  THEN 'Tắc nghẽn'
                WHEN r.avg_speed < %(slow)s  THEN 'Chậm'
                ELSE 'Thông thoáng'
            END                                          AS congestion_label,
            
            -- Ước tính thời gian chờ đợi trung bình
            CASE WHEN r.avg_speed < 1 THEN 120
                 WHEN r.avg_speed < %(cong)s THEN 60
                 WHEN r.avg_speed < %(slow)s THEN 20
                 ELSE 5
            END::DOUBLE PRECISION                        AS avg_waiting_time,
            
            -- Chiều dài hàng đợi (áp dụng bảo vệ chia cho 0)
            GREATEST(0, (%(cong)s - r.avg_speed) / NULLIF(%(cong)s, 0.0) * r.veh_count)
                                                         AS queue_length,
            
            s.red_duration                               AS scheduled_red_duration,
            
            -- LOGIC ĐÁNH GIÁ HIỆU QUẢ MỚI (Inference Logic):
            -- Nếu có từ 3 xe trở lên đang lết với vận tốc cực chậm (< 5km/h) 
            -- chứng tỏ chu kỳ đèn tĩnh hiện tại không đủ giải phóng luồng xe.
            CASE WHEN
                r.veh_count >= 3
                AND r.avg_speed < 5.0
            THEN TRUE ELSE FALSE END                     AS is_inefficient

        FROM recent r
        LEFT JOIN sched s
            ON s.traffic_light_id = r.traffic_light_id
            AND s.applicable_days = r.day_type
        LEFT JOIN cur_time ct ON TRUE

        ON CONFLICT (traffic_light_id, analysis_time) DO NOTHING
    """, {"cong": SPEED_CONGESTED_KMH, "slow": SPEED_SLOW_KMH})

    count = cur.rowcount
    conn.commit()
    cur.close()
    if count > 0:
        log.info(f"  Phân tích: {count} kết quả → congestion_analysis")
    return count


# ══════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════
def main():
    log.info(f"🔄 ETL Worker khởi động | Môi trường: {ENV_LABEL}")
    s3   = get_s3()
    conn = get_db()
    last_run = datetime.now(timezone.utc) - timedelta(hours=1)

    try:
        while True:
            run_start = datetime.now(timezone.utc)
            log.info(f"\n{'─'*50}")
            log.info(f"⚙️  ETL [{ENV_LABEL}] lúc {run_start.strftime('%H:%M:%S')}")

            try:
                # Chỉ xử lý luồng duy nhất là GPS
                gps_n = etl_gps(s3, conn, last_run)
                etl_analysis(conn)

                if IS_LOCAL:
                    cleanup_cur = conn.cursor()
                    cleanup_cur.execute("DELETE FROM vehicle_gps WHERE recorded_at < NOW() - INTERVAL '7 days'")
                    conn.commit()
                    cleanup_cur.close()

                if gps_n == 0:
                    log.info("  Không có dữ liệu GPS mới.")
            except psycopg2.OperationalError:
                log.warning("⚠️ Mất kết nối DB — reconnect...")
                try:
                    conn.close()
                except Exception:
                    pass
                conn = get_db()
            except Exception as e:
                log.error(f"❌ ETL lỗi: {e}", exc_info=True)
                try:
                    conn.rollback()
                except Exception:
                    pass

            last_run = run_start
            elapsed  = (datetime.now(timezone.utc) - run_start).total_seconds()
            sleep_s  = max(0, ETL_INTERVAL_SEC - elapsed)
            log.info(f"💤 Chờ {sleep_s:.0f}s...")
            time.sleep(sleep_s)

    except KeyboardInterrupt:
        log.info(f"\n🛑 Dừng ETL Worker [{ENV_LABEL}].")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
