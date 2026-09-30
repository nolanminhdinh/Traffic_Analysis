"""
app.py — Flask API Gateway cho Traffic Analysis Pipeline
═══════════════════════════════════════════════════════════════════════════
Đóng vai trò Cổng nạp dữ liệu (Ingestion Gateway):
  Flutter Mobile App / Web Client -> POST /save_gps -> Apache Kafka (gps_stream_topic)

Tính năng chính:
  1. Kiểm tra tính hợp lệ của dữ liệu GPS (Coordinates validation).
  2. Chuẩn hóa vận tốc từ m/s (chuẩn thiết bị GPS) sang km/h (chuẩn phân tích).
  3. Gắn nhãn bối cảnh không gian thời gian (day_type, time_window).
  4. Đẩy bất đồng bộ vào Apache Kafka Topic với cơ chế Delivery Callback.
  5. Cung cấp Health Check endpoint & Dashboard trạng thái.
═══════════════════════════════════════════════════════════════════════════
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer, KafkaException
from dotenv import load_dotenv
from flask import Flask, jsonify, render_template, render_template_string, request
from flask_cors import CORS

# Load biến môi trường từ file .env
env_path = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=env_path)

# Cấu hình Logging chuẩn hóa
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] (%(name)s) %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("TrafficAPI")

# Khởi tạo Flask Application
app = Flask(__name__, template_folder="templates")
CORS(app)

# Đọc cấu hình từ biến môi trường
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
GPS_TOPIC = os.getenv("KAFKA_TOPIC_GPS", "gps_stream_topic")
FLASK_HOST = os.getenv("FLASK_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("FLASK_PORT", 5000))
FLASK_DEBUG = os.getenv("FLASK_DEBUG", "False").lower() in ("true", "1", "yes")

# Khởi tạo Kafka Producer
try:
    producer = Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "client.id": "traffic-api-gateway",
        "acks": "all",
        "retries": 3,
        "socket.timeout.ms": 5000,
    })
    log.info(f"Connected to Kafka broker at: {KAFKA_BOOTSTRAP}")
except Exception as e:
    log.error(f"Failed to initialize Kafka Producer: {e}")
    producer = None


def _delivery_callback(err, msg):
    """Callback xử lý trạng thái tin nhắn Kafka."""
    if err:
        log.error(f"Kafka Delivery Failed on topic [{msg.topic()}]: {err}")
    else:
        log.debug(f"Kafka Delivered to [{msg.topic()}] partition [{msg.partition()}] offset={msg.offset()}")


@app.route("/", methods=["GET"])
def index():
    """Trang chủ hiển thị trạng thái API Gateway và hướng dẫn sử dụng."""
    try:
        if (Path(app.template_folder) / "index.html").exists():
            return render_template("index.html")
    except Exception:
        pass

    # Fallback giao diện HTML hiện đại nếu chưa tạo template riêng
    html_content = """
    <!DOCTYPE html>
    <html lang="vi">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Traffic Analysis API Gateway</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
                   background: #0f172a; color: #f8fafc; margin: 0; padding: 40px 20px; display: flex; justify-content: center; }
            .card { background: #1e293b; border-radius: 16px; padding: 32px; max-width: 700px; width: 100%; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
            h1 { color: #38bdf8; margin-top: 0; font-size: 26px; }
            .badge { display: inline-block; padding: 6px 14px; border-radius: 999px; font-size: 13px; font-weight: 600; background: #0284c7; color: white; margin-bottom: 20px; }
            p { line-height: 1.6; color: #cbd5e1; }
            code { background: #334155; padding: 3px 8px; border-radius: 6px; font-family: monospace; color: #38bdf8; }
            pre { background: #0f172a; padding: 16px; border-radius: 8px; overflow-x: auto; color: #a5f3fc; font-size: 14px; }
            .status-box { border-left: 4px solid #10b981; padding: 12px 16px; background: #064e3b33; margin: 20px 0; border-radius: 4px; }
        </style>
    </head>
    <body>
        <div class="card">
            <span class="badge">Traffic Analysis Project</span>
            <h1>Traffic Ingestion API Gateway</h1>
            <div class="status-box">
                <strong>Trạng thái:</strong> Gateway đang hoạt động và sẵn sàng nhận dữ liệu GPS.
            </div>
            <p>API Endpoint: <code>POST /save_gps</code></p>
            <p>Định dạng payload mẫu (JSON):</p>
            <pre>{
  "gps_id": "GPS_001",
  "timestamp": "2026-09-30 18:30:00",
  "latitude": 21.0664,
  "longitude": 105.7815,
  "speed": 4.5,
  "traffic_light_id": "TL001",
  "day_type": "weekday",
  "time_window": "peak_hour"
}</pre>
            <p>Kiểm tra tình trạng sức khỏe server: <code><a href="/health" style="color: #38bdf8;">GET /health</a></code></p>
        </div>
    </body>
    </html>
    """
    return render_template_string(html_content)


@app.route("/save_gps", methods=["POST"])
def save_gps():
    """
    Nhận dữ liệu GPS từ Flutter App hoặc thiết bị IoT,
    chuẩn hóa và đẩy vào Apache Kafka.
    """
    if producer is None:
        return jsonify({
            "status": "error",
            "message": "Kafka Producer chưa được khởi tạo. Vui lòng kiểm tra Kafka broker."
        }), 503

    try:
        raw = request.get_json(force=True, silent=True)
        if not raw:
            return jsonify({"status": "error", "message": "Payload JSON rỗng hoặc không đúng định dạng"}), 400

        # Kiểm tra các trường bắt buộc
        required_fields = ["gps_id", "timestamp", "latitude", "longitude"]
        missing = [f for f in required_fields if f not in raw]
        if missing:
            return jsonify({
                "status": "error",
                "message": f"Thiếu các trường bắt buộc: {missing}"
            }), 400

        # Kiểm tra tính hợp lệ của tọa độ
        lat = float(raw["latitude"])
        lon = float(raw["longitude"])
        if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
            return jsonify({
                "status": "error",
                "message": f"Tọa độ không hợp lệ: lat={lat}, lon={lon}"
            }), 400

        # Chuẩn hóa vận tốc:
        # Nếu thiết bị gửi speed dạng m/s (nhỏ hơn 50) hoặc gửi trực tiếp km/h
        raw_speed = float(raw.get("speed", 0.0))
        # Nếu có cờ speed_unit=='kmh' thì giữ nguyên, mặc định geolocator trả m/s -> nhân 3.6
        if raw.get("speed_unit") == "kmh" or "speed_kmh" in raw:
            speed_kmh = round(float(raw.get("speed_kmh", raw_speed)), 2)
        else:
            speed_kmh = round(max(0.0, min(raw_speed * 3.6, 200.0)), 2)

        # Trích xuất bối cảnh thời gian nếu chưa có
        timestamp_str = str(raw["timestamp"])
        day_type = raw.get("day_type")
        time_window = raw.get("time_window")

        if not day_type or not time_window:
            try:
                dt = datetime.fromisoformat(timestamp_str.replace(" ", "T"))
                if not day_type:
                    day_type = "weekend" if dt.weekday() >= 5 else "weekday"
                if not time_window:
                    time_window = "peak_hour" if dt.hour in (6, 7, 8, 16, 17, 18, 19) else "off_peak"
            except Exception:
                day_type = day_type or "weekday"
                time_window = time_window or "off_peak"

        # Chuẩn bị message đẩy vào Kafka
        message = {
            "gps_id":           str(raw["gps_id"]),
            "timestamp":        timestamp_str,
            "latitude":         lat,
            "longitude":        lon,
            "speed_kmh":        speed_kmh,
            "traffic_light_id": str(raw.get("traffic_light_id", "unknown")),
            "day_type":         day_type,
            "time_window":      time_window,
            "received_at":      datetime.now(timezone.utc).isoformat(),
        }

        # Đẩy dữ liệu vào Kafka
        producer.produce(
            GPS_TOPIC,
            key=str(message["gps_id"]),
            value=json.dumps(message).encode("utf-8"),
            callback=_delivery_callback
        )
        producer.poll(0)

        log.info(f"📍 GPS [{message['gps_id']}] | ({lat:.5f}, {lon:.5f}) | {speed_kmh} km/h | Light: {message['traffic_light_id']}")

        return jsonify({
            "status": "queued",
            "message": f"Dữ liệu {message['gps_id']} đã được đưa vào Kafka",
            "data": message
        }), 200

    except (ValueError, TypeError) as e:
        return jsonify({"status": "error", "message": f"Kiểu dữ liệu không hợp lệ: {str(e)}"}), 400
    except KafkaException as e:
        log.error(f"Kafka broker exception: {e}")
        return jsonify({"status": "error", "message": f"Lỗi Kafka broker: {str(e)}"}), 503
    except Exception as e:
        log.error(f"Lỗi không xác định khi lưu GPS: {e}", exc_info=True)
        return jsonify({"status": "error", "message": f"Lỗi máy chủ: {str(e)}"}), 500


@app.route("/health", methods=["GET"])
def health():
    """Kiểm tra trạng thái kết nối tới Kafka và Flask."""
    kafka_status = "unreachable"
    if producer is not None:
        try:
            metadata = producer.list_topics(timeout=3.0)
            if GPS_TOPIC in metadata.topics or len(metadata.topics) > 0:
                kafka_status = "connected"
        except Exception as e:
            kafka_status = f"error: {str(e)}"

    is_healthy = kafka_status == "connected"
    return jsonify({
        "status": "healthy" if is_healthy else "degraded",
        "service": "traffic-api-gateway",
        "kafka_broker": KAFKA_BOOTSTRAP,
        "kafka_topic": GPS_TOPIC,
        "kafka_status": kafka_status,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }), 200 if is_healthy else 503


if __name__ == "__main__":
    log.info("=" * 65)
    log.info(f"🚀 Khởi động Flask API Gateway trên http://{FLASK_HOST}:{FLASK_PORT}")
    log.info(f"📍 Kafka Broker: {KAFKA_BOOTSTRAP} | Topic: {GPS_TOPIC}")
    log.info("=" * 65)
    app.run(host=FLASK_HOST, port=FLASK_PORT, debug=FLASK_DEBUG)
