from flask import Flask, render_template, request, jsonify
from flask_cors import CORS
import psycopg2
import folium
from datetime import datetime

app = Flask(__name__)
CORS(app)  # Cho phép Flutter app kết nối từ thiết bị khác

# --- CẤU HÌNH KẾT NỐI DATABASE ---
def get_connection():
    return psycopg2.connect(
        host="ep-old-voice-a1mw52bq-pooler.ap-southeast-1.aws.neon.tech",
        port="5432",
        database="neondb",  # Database mặc định của Neon
        user="neondb_owner",  
        password="npg_MQb4CymvOSs5",
        sslmode="require"  # Bắt buộc SSL cho Neon
    )

@app.route("/")
def index():    
    return render_template("index.html")

@app.route("/save_gps", methods=["POST"])
def save_gps():
    conn = None
    try:
        # 1. Nhận dữ liệu từ thiết bị gửi lên
        gps_data = request.get_json()
        print("=" * 50)
        print(" Dữ liệu GPS nhận được:")
        print(f"   Device: {gps_data.get('device_id')}")
        print(f"   Lat/Lon: {gps_data.get('latitude')}, {gps_data.get('longitude')}")
        print(f"   Speed: {gps_data.get('speed')} km/h")
        print(f"   Accuracy: {gps_data.get('accuracy_m')} m")
        print(f"   Time: {gps_data.get('timestamp')}")
        print("=" * 50)

        # 2. Xử lý timestamp
        timestamp_str = gps_data["timestamp"]

        conn = get_connection()
        cur = conn.cursor()

        # 3. Câu lệnh SQL - chỉ lưu vehicle_id, timestamp và geom
        # speed_kmh và heading để NULL vì không cần thiết hiện tại
        query = """
            INSERT INTO vehicle_gps 
            (vehicle_id, timestamp, speed_kmh, heading, geom)
            VALUES (%s, %s, NULL, NULL, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            RETURNING gps_id;
        """
        
        cur.execute(query, (
            gps_data["device_id"],      # vehicle_id
            gps_data["timestamp"],      # timestamp
            gps_data["longitude"],      # X cho geom (lon, lat)
            gps_data["latitude"]        # Y cho geom
        ))

        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

        print(f"✅ Đã lưu thành công GPS_ID: {new_id} vào database")

        # 4. Tạo bản đồ báo cáo
        m = folium.Map(
            location=[gps_data["latitude"], gps_data["longitude"]], 
            zoom_start=17
        )
        
        popup_text = f"""
        <b>GPS ID:</b> {new_id}<br>
        <b>Xe:</b> {gps_data['device_id']}<br>
        <b>Tốc độ:</b> {gps_data['speed']:.1f} km/h<br>
        <b>Độ chính xác:</b> {gps_data.get('accuracy_m', 'N/A')} m<br>
        <b>Thời gian:</b> {gps_data['timestamp']}
        """
        
        folium.Marker(
            [gps_data["latitude"], gps_data["longitude"]], 
            popup=popup_text,
            icon=folium.Icon(color='blue', icon='car', prefix='fa')
        ).add_to(m)

        return jsonify({
            "status": "success",
            "gps_id": new_id,
            "device_id": gps_data["device_id"],
            "map_html": m._repr_html_(),
            "message": f"✅ Đã lưu GPS_ID {new_id} vào database thành công!",
            "data_saved": {
                "latitude": gps_data["latitude"],
                "longitude": gps_data["longitude"],
                "speed": gps_data["speed"],
                "timestamp": gps_data["timestamp"]
            }
        }), 200

    except KeyError as e:
        error_msg = f"❌ Thiếu trường dữ liệu: {str(e)}"
        print(error_msg)
        return jsonify({
            "status": "error",
            "error": error_msg,
            "message": "Vui lòng kiểm tra format dữ liệu gửi lên"
        }), 400

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        error_msg = f"❌ Lỗi Database: {str(e)}"
        print(error_msg)
        return jsonify({
            "status": "error",
            "error": error_msg,
            "message": "Không thể lưu dữ liệu vào database"
        }), 500

    except Exception as e:
        if conn:
            conn.rollback()
        error_msg = f"❌ Lỗi không xác định: {str(e)}"
        print(error_msg)
        return jsonify({
            "status": "error",
            "error": error_msg
        }), 500
        
    finally:
        if conn:
            conn.close()

@app.route("/health", methods=["GET"])
def health_check():
    """Endpoint để kiểm tra server có hoạt động không"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM vehicle_gps;")
        count = cur.fetchone()[0]
        cur.close()
        conn.close()
        
        return jsonify({
            "status": "healthy",
            "message": "Server đang hoạt động",
            "database": "connected",
            "total_records": count
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "message": "Lỗi kết nối database",
            "error": str(e)
        }), 500

if __name__ == "__main__":
    print("\n" + "="*60)
    print("🚀 GPS Collector Flask Server")
    print("="*60)
    print("📍 Server URL: http://127.0.0.1:5000")
    print("📍 Emulator URL: http://10.0.2.2:5000")
    print("📍 Health Check: http://127.0.0.1:5000/health")
    print("="*60 + "\n")
    
    # Lắng nghe trên tất cả network interfaces để thiết bị thật có thể kết nối
    app.run(host="0.0.0.0", port=5000, debug=True)
