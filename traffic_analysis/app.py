"""
app.py — Flask API Gateway
══════════════════════════════════════════════════════════════
Flutter App → POST /save_gps → Kafka (gps_stream_topic)

JSON Flutter gửi lên (khớp SQLite vehicle_gps của app):
{
    "gps_id":           "GPS_1763886879",
    "timestamp":        "2025-11-23 15:34:39",
    "latitude":         21.083385,
    "longitude":        105.780872,
    "speed":            15.45,            ← m/s (Geolocator trả về m/s)
    "traffic_light_id": "TL005",
    "day_type":         "weekday",
    "time_window":      "peak_hour"
}
══════════════════════════════════════════════════════════════
"""
import json
import logging
from datetime import datetime, timezone

from confluent_kafka import Producer, KafkaException
from flask import Flask, jsonify, render_template, request
from flask_cors import CORS

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

app       = Flask(__name__)
CORS(app)
producer  = Producer({"bootstrap.servers": "localhost:9092"})
GPS_TOPIC = "gps_stream_topic"


def _delivery_cb(err, msg):
    if err:
        log.error(f"❌ Kafka failed: {err}")
    else:
        log.debug(f"✅ Kafka [{msg.topic()}] offset={msg.offset()}")


@app.route("/save_gps", methods=["POST"])
def save_gps():
    try:
        raw = request.get_json(force=True)
        if not raw:
            return jsonify({"status": "error", "message": "Body rỗng"}), 400

        # Validate trường bắt buộc (khớp SQLite app)
        missing = [f for f in ["gps_id", "timestamp", "latitude", "longitude"]
                   if f not in raw]
        if missing:
            return jsonify({"status": "error",
                            "message": f"Thiếu trường: {missing}"}), 400

        lat = float(raw["latitude"])
        lon = float(raw["longitude"])
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return jsonify({"status": "error",
                            "message": "Tọa độ không hợp lệ"}), 400

        # speed từ Geolocator là m/s → convert sang km/h
        speed_ms  = float(raw.get("speed", 0))
        speed_kmh = round(max(0.0, min(speed_ms * 3.6, 200.0)), 2)

        message = {
            "gps_id":           str(raw["gps_id"]),
            "timestamp":        raw["timestamp"],          # "2025-11-23 15:34:39"
            "latitude":         lat,
            "longitude":        lon,
            "speed_kmh":        speed_kmh,                 # đã convert
            "traffic_light_id": raw.get("traffic_light_id", "unknown"),
            "day_type":         raw.get("day_type", "weekday"),
            "time_window":      raw.get("time_window", "off_peak"),
            "received_at":      datetime.now(timezone.utc).isoformat(),
        }

        producer.produce(GPS_TOPIC, key=message["gps_id"],
                         value=json.dumps(message).encode(),
                         callback=_delivery_cb)
        producer.poll(0)

        log.info(f"📍 {message['gps_id']} | {lat:.4f},{lon:.4f} | "
                 f"{speed_kmh}km/h | light={message['traffic_light_id']} "
                 f"| {message['time_window']}")

        return jsonify({
            "status":  "queued",
            "message": f"✅ {message['gps_id']} đã vào Kafka",
            "data":    message,
        }), 200

    except (ValueError, TypeError) as e:
        return jsonify({"status": "error",
                        "message": f"Dữ liệu không hợp lệ: {e}"}), 400
    except KafkaException as e:
        return jsonify({"status": "error",
                        "message": "Kafka không khả dụng"}), 503
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/health")
def health():
    try:
        ok = GPS_TOPIC in producer.list_topics(timeout=3).topics
    except Exception:
        ok = False
    return jsonify({
        "flask":  "ok",
        "kafka":  "ok" if ok else "unreachable",
        "status": "healthy" if ok else "degraded",
    }), 200 if ok else 503


@app.route("/")
def index():
    return render_template("index.html")


if __name__ == "__main__":
    print("🚀 Flask API Gateway | http://0.0.0.0:5000")
    print("📍 Emulator: http://10.0.2.2:5000")
    app.run(host="0.0.0.0", port=5000, debug=True)
