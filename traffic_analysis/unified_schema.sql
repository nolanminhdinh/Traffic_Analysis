-- Chạy trên NEON CLOUD:
-- ================================================================
-- unified_schema.sql — Schema hợp nhất (Phiên bản Suy luận Tĩnh - NCKH)
-- ================================================================
-- Hỗ trợ: PostGIS, Phân vùng thời gian (Partitioning), Trigger tự động
-- Tương thích hoàn toàn với dữ liệu thực tế từ ứng dụng Flutter
-- Đã loại bỏ: Dữ liệu sự kiện đèn thời gian thực (raw_traffic_lights)
-- ================================================================

CREATE EXTENSION IF NOT EXISTS postgis;

-- ================================================================
-- 1. intersections — Dữ liệu tĩnh về các nút giao thông
-- ================================================================
CREATE TABLE IF NOT EXISTS public.intersections (
    intersection_id   SERIAL           PRIMARY KEY,
    intersection_name TEXT             NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    geom              GEOMETRY(Point, 4326),
    description       TEXT
);

CREATE INDEX IF NOT EXISTS idx_intersections_geom
    ON intersections USING GIST (geom);

-- Trigger tự điền geom từ lat/lon
CREATE OR REPLACE FUNCTION fill_intersection_geom()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.geom IS NULL AND NEW.latitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_intersection_geom ON intersections;
CREATE TRIGGER trg_intersection_geom
    BEFORE INSERT OR UPDATE ON intersections
    FOR EACH ROW EXECUTE FUNCTION fill_intersection_geom();

INSERT INTO intersections (intersection_name, latitude, longitude, description) VALUES
    ('Ngã tư Xuân Đỉnh',       21.0664, 105.7815, 'Phạm Văn Đồng - Xuân Đỉnh'),
    ('Ngã tư Cổ Nhuế',          21.0562, 105.7823, 'Phạm Văn Đồng - Cổ Nhuế'),
    ('Ngã tư Hoàng Quốc Việt',  21.0458, 105.7831, 'Phạm Văn Đồng - Hoàng Quốc Việt'),
    ('Ngã tư Mai Dịch',          21.0368, 105.7795, 'Phạm Văn Đồng - Mai Dịch'),
    ('Khu vực Xuân Đỉnh thực',  21.0834, 105.7808, 'Điểm thu thập thực tế từ app')
ON CONFLICT DO NOTHING;

-- ================================================================
-- 2. traffic_lights — Thông tin danh mục đèn giao thông
-- ================================================================
CREATE TABLE IF NOT EXISTS public.traffic_lights (
    traffic_light_id  VARCHAR(50)       PRIMARY KEY,
    intersection_id   INT               REFERENCES intersections(intersection_id),
    intersection_name TEXT,
    latitude          DOUBLE PRECISION,
    longitude         DOUBLE PRECISION,
    geom              GEOMETRY(Point, 4326),
    direction_degree  SMALLINT,
    default_green     SMALLINT,
    default_yellow    SMALLINT,
    default_red       SMALLINT,
    active_status     BOOLEAN           DEFAULT TRUE,
    is_adaptive       BOOLEAN           DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_traffic_lights_geom
    ON traffic_lights USING GIST (geom);

CREATE OR REPLACE FUNCTION fill_traffic_light_geom()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.geom IS NULL AND NEW.latitude IS NOT NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_traffic_light_geom ON traffic_lights;
CREATE TRIGGER trg_traffic_light_geom
    BEFORE INSERT OR UPDATE ON traffic_lights
    FOR EACH ROW EXECUTE FUNCTION fill_traffic_light_geom();

INSERT INTO traffic_lights
    (traffic_light_id, intersection_id, intersection_name, latitude, longitude,
     direction_degree, default_green, default_yellow, default_red, is_adaptive)
VALUES
    ('TL001', 1, 'Ngã tư Xuân Đỉnh',       21.0664, 105.7815, 180, 45, 3, 60, FALSE),
    ('TL002', 2, 'Ngã tư Cổ Nhuế',          21.0562, 105.7823, 180, 40, 3, 55, FALSE),
    ('TL003', 3, 'Ngã tư Hoàng Quốc Việt',  21.0458, 105.7831, 180, 50, 3, 65, FALSE),
    ('TL004', 4, 'Ngã tư Mai Dịch',          21.0368, 105.7795, 180, 45, 3, 60, FALSE),
    ('TL005', 5, 'Khu vực Xuân Đỉnh thực',  21.0834, 105.7808, 180, 45, 3, 60, FALSE)
ON CONFLICT DO NOTHING;

-- ================================================================
-- 3. light_schedule — Lịch trình điều khiển đèn theo khung giờ
-- ================================================================
CREATE TABLE IF NOT EXISTS public.light_schedule (
    schedule_id       SERIAL          PRIMARY KEY,
    traffic_light_id  VARCHAR(50)     REFERENCES traffic_lights(traffic_light_id),
    start_time        TIME            NOT NULL,
    end_time          TIME            NOT NULL,
    green_duration    SMALLINT        NOT NULL,
    yellow_duration   SMALLINT        NOT NULL,
    red_duration      SMALLINT        NOT NULL,
    applicable_days   VARCHAR(10)     NOT NULL DEFAULT 'weekday',  -- weekday | weekend
    control_mode      VARCHAR(20)     NOT NULL DEFAULT 'fixed',    -- fixed | adaptive
    UNIQUE (traffic_light_id, start_time, applicable_days)
);

INSERT INTO light_schedule
    (traffic_light_id, start_time, end_time, green_duration, yellow_duration, red_duration, applicable_days)
VALUES
    ('TL001','06:00','09:00',45,3,60,'weekday'), ('TL001','09:00','16:00',35,3,45,'weekday'),
    ('TL001','16:00','20:00',50,3,65,'weekday'), ('TL001','20:00','06:00',30,3,40,'weekday'),
    ('TL002','06:00','09:00',40,3,55,'weekday'), ('TL002','09:00','16:00',30,3,40,'weekday'),
    ('TL002','16:00','20:00',45,3,60,'weekday'), ('TL002','20:00','06:00',25,3,35,'weekday'),
    ('TL003','06:00','09:00',50,3,65,'weekday'), ('TL003','09:00','16:00',40,3,50,'weekday'),
    ('TL003','16:00','20:00',55,3,70,'weekday'), ('TL003','20:00','06:00',35,3,45,'weekday'),
    ('TL004','06:00','09:00',45,3,60,'weekday'), ('TL004','09:00','16:00',35,3,45,'weekday'),
    ('TL004','16:00','20:00',50,3,65,'weekday'), ('TL004','20:00','06:00',30,3,40,'weekday'),
    ('TL005','06:00','09:00',45,3,60,'weekday'), ('TL005','09:00','16:00',35,3,45,'weekday'),
    ('TL005','16:00','20:00',50,3,65,'weekday'), ('TL005','20:00','06:00',30,3,40,'weekday'),
    ('TL005','06:00','20:00',40,3,50,'weekend'), ('TL005','20:00','06:00',25,3,35,'weekend')
ON CONFLICT DO NOTHING;

-- ================================================================
-- 4. time_dimension — Chiều thời gian phân tích
-- ================================================================
CREATE TABLE IF NOT EXISTS public.time_dimension (
    time_id         SERIAL      PRIMARY KEY,
    full_timestamp  TIMESTAMPTZ NOT NULL UNIQUE,
    date_val        DATE        NOT NULL,
    year            SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    day             SMALLINT    NOT NULL,
    hour            SMALLINT    NOT NULL,
    minute          SMALLINT    NOT NULL DEFAULT 0,
    day_of_week     SMALLINT    NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    quarter         SMALLINT    NOT NULL,
    is_weekend      BOOLEAN     NOT NULL,
    is_holiday      BOOLEAN     NOT NULL DEFAULT FALSE,
    day_type        VARCHAR(10) NOT NULL,           -- 'weekday' | 'weekend'
    time_window     VARCHAR(10) NOT NULL,           -- 'peak_hour' | 'off_peak'
    season          VARCHAR(10)
);

-- Điền time_dimension theo từng giờ từ 2025-01-01 đến 2027-12-31
INSERT INTO time_dimension
    (full_timestamp, date_val, year, month, day, hour, minute,
     day_of_week, week_of_year, quarter,
     is_weekend, day_type, time_window, season)
SELECT
    ts,
    ts::DATE,
    EXTRACT(YEAR    FROM ts)::SMALLINT,
    EXTRACT(MONTH   FROM ts)::SMALLINT,
    EXTRACT(DAY     FROM ts)::SMALLINT,
    EXTRACT(HOUR    FROM ts)::SMALLINT,
    0,
    EXTRACT(DOW     FROM ts)::SMALLINT,
    EXTRACT(WEEK    FROM ts)::SMALLINT,
    EXTRACT(QUARTER FROM ts)::SMALLINT,
    EXTRACT(DOW FROM ts) IN (0, 6),
    CASE WHEN EXTRACT(DOW FROM ts) IN (0,6) THEN 'weekend' ELSE 'weekday' END,
    CASE WHEN EXTRACT(HOUR FROM ts) IN (6,7,8,16,17,18,19)
         THEN 'peak_hour' ELSE 'off_peak' END,
    CASE EXTRACT(MONTH FROM ts)::INT
        WHEN 3 THEN 'spring' WHEN 4 THEN 'spring' WHEN 5 THEN 'spring'
        WHEN 6 THEN 'summer' WHEN 7 THEN 'summer' WHEN 8 THEN 'summer'
        WHEN 9 THEN 'autumn' WHEN 10 THEN 'autumn' WHEN 11 THEN 'autumn'
        ELSE 'winter'
    END
FROM generate_series(
    '2025-01-01 00:00:00+00'::TIMESTAMPTZ,
    '2027-12-31 23:00:00+00'::TIMESTAMPTZ,
    '1 hour'::INTERVAL
) AS ts
ON CONFLICT (full_timestamp) DO NOTHING;

-- ================================================================
-- 5. vehicle_gps — Dữ liệu luồng phương tiện (Phân vùng theo tháng)
-- ================================================================
CREATE TABLE IF NOT EXISTS public.vehicle_gps (
    gps_id            TEXT             NOT NULL,
    vehicle_id        VARCHAR(50),
    recorded_at       TIMESTAMPTZ      NOT NULL,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    geom              GEOMETRY(Point, 4326),
    speed_kmh         DOUBLE PRECISION NOT NULL DEFAULT 0,
    heading           DOUBLE PRECISION,
    traffic_light_id  VARCHAR(50)      REFERENCES traffic_lights(traffic_light_id),
    day_type          VARCHAR(10),
    time_window       VARCHAR(10),
    time_id           INT              REFERENCES time_dimension(time_id),
    PRIMARY KEY (gps_id, recorded_at)
) PARTITION BY RANGE (recorded_at);

-- Duy trì đầy đủ các dải phân vùng thời gian
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2025m11
    PARTITION OF vehicle_gps FOR VALUES FROM ('2025-11-01') TO ('2025-12-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2025m12
    PARTITION OF vehicle_gps FOR VALUES FROM ('2025-12-01') TO ('2026-01-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m01
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m02
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m03
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-03-01') TO ('2026-04-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m04
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-04-01') TO ('2026-05-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m05
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m06
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-06-01') TO ('2026-07-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m07
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-07-01') TO ('2026-08-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m08
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-08-01') TO ('2026-09-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m09
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-09-01') TO ('2026-10-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m10
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m11
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-11-01') TO ('2026-12-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_y2026m12
    PARTITION OF vehicle_gps FOR VALUES FROM ('2026-12-01') TO ('2027-01-01');
CREATE TABLE IF NOT EXISTS public.vehicle_gps_default
    PARTITION OF vehicle_gps DEFAULT;

CREATE INDEX IF NOT EXISTS idx_vgps_geom
    ON vehicle_gps USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_vgps_recorded_at
    ON vehicle_gps (recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vgps_vehicle_id
    ON vehicle_gps (vehicle_id);
CREATE INDEX IF NOT EXISTS idx_vgps_traffic_light
    ON vehicle_gps (traffic_light_id, recorded_at DESC);
CREATE INDEX IF NOT EXISTS idx_vgps_timewindow
    ON vehicle_gps (time_window, day_type, recorded_at DESC);

-- Trigger: Tự động điền Geom, Vehicle_ID và Time_ID khi nạp dữ liệu
CREATE OR REPLACE FUNCTION fill_vgps_defaults()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.geom IS NULL THEN
        NEW.geom := ST_SetSRID(ST_MakePoint(NEW.longitude, NEW.latitude), 4326);
    END IF;
    IF NEW.vehicle_id IS NULL THEN
        NEW.vehicle_id := NEW.gps_id;
    END IF;
    IF NEW.time_id IS NULL THEN
        SELECT time_id INTO NEW.time_id
        FROM time_dimension
        WHERE date_val = NEW.recorded_at::DATE
          AND hour = EXTRACT(HOUR FROM NEW.recorded_at)::SMALLINT
        LIMIT 1;
    END IF;
    IF NEW.day_type IS NULL THEN
        NEW.day_type := CASE
            WHEN EXTRACT(DOW FROM NEW.recorded_at) IN (0,6) THEN 'weekend'
            ELSE 'weekday'
        END;
    END IF;
    IF NEW.time_window IS NULL THEN
        NEW.time_window := CASE
            WHEN EXTRACT(HOUR FROM NEW.recorded_at) IN (6,7,8,16,17,18,19)
            THEN 'peak_hour' ELSE 'off_peak'
        END;
    END IF;
    RETURN NEW;
END;
$$;
DROP TRIGGER IF EXISTS trg_vgps_defaults ON vehicle_gps;
CREATE TRIGGER trg_vgps_defaults
    BEFORE INSERT ON vehicle_gps
    FOR EACH ROW EXECUTE FUNCTION fill_vgps_defaults();

-- ================================================================
-- 6. congestion_analysis — Bảng đích lưu trữ phân tích suy luận
--    Được tối ưu lại: loại bỏ các trường đo đếm thực tế của đèn
-- ================================================================
CREATE TABLE IF NOT EXISTS public.congestion_analysis (
    analysis_id             SERIAL           PRIMARY KEY,
    traffic_light_id        VARCHAR(50)      REFERENCES traffic_lights(traffic_light_id),
    analysis_time           TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    time_id                 INT              REFERENCES time_dimension(time_id),
    time_window             VARCHAR(10),     -- 'peak_hour' | 'off_peak'
    day_type                VARCHAR(10),     -- 'weekday' | 'weekend'
    avg_speed_kmh           DOUBLE PRECISION,
    vehicle_count           INT,
    congestion_level        DOUBLE PRECISION
        CHECK (congestion_level BETWEEN 0 AND 100),
    congestion_label        VARCHAR(20),     -- 'Tắc nghẽn'|'Chậm'|'Thông thoáng'
    avg_waiting_time        DOUBLE PRECISION, -- ước tính theo giây
    queue_length            DOUBLE PRECISION, -- số xe ước tính
    scheduled_red_duration  SMALLINT,        -- giây, lấy từ cấu hình light_schedule tĩnh
    is_inefficient          BOOLEAN          DEFAULT FALSE,
    UNIQUE (traffic_light_id, analysis_time)
);

CREATE INDEX IF NOT EXISTS idx_ca_traffic_light
    ON congestion_analysis (traffic_light_id, analysis_time DESC);
CREATE INDEX IF NOT EXISTS idx_ca_inefficient
    ON congestion_analysis (is_inefficient, analysis_time DESC);
CREATE INDEX IF NOT EXISTS idx_ca_timewindow
    ON congestion_analysis (time_window, day_type, analysis_time DESC);

-- ================================================================
\echo ''
\echo ' Unified schema (Phiên bản Suy luận Tĩnh) khởi tạo thành công!'
\dt public.*
