from flask import Flask, render_template, request, jsonify
import psycopg2
import folium
from datetime import datetime

app = Flask(__name__)

# --- CẤU HÌNH KẾT NỐI DATABASE ---
def get_connection():
    return psycopg2.connect(
        host="ep-old-voice-a1mw52bq-pooler.ap-southeast-1.aws.neon.tech",  # Host từ báo cáo
        port="5432",
        database="TraficDB_Cloud",  # Tên database trên Neon
        user="neondb_owner",  # Username từ báo cáo
        password="npg_MQb4CymvOSs5"  # Ib Minh để lấy mật khẩu, hoặc dùng mật khẩu của bạn nếu đã set
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
        print("Dữ liệu nhận:", gps_data)

        # 2. Xử lý dữ liệu để khớp với Database
        # Lấy ngày từ timestamp (ví dụ: '2025-12-07T14:30:00' -> '2025-12-07')
        timestamp_str = gps_data["timestamp"]
        date_ref_val = timestamp_str.split("T")[0] 

        conn = get_connection()
        cur = conn.cursor()

        # 3. Câu lệnh SQL khớp với 7 cột trong ảnh bạn gửi:
        # (gps_id tự tăng nên không điền)
        # vehicle_id, timestamp, date_ref, latitude, longitude, speed, geom
        query = """
            INSERT INTO vehicle_gps 
            (vehicle_id, timestamp, date_ref, latitude, longitude, speed, geom)
            VALUES (%s, %s, %s, %s, %s, %s, ST_SetSRID(ST_MakePoint(%s, %s), 4326))
            RETURNING gps_id;
        """
        
        cur.execute(query, (
            gps_data["device_id"],  # Lưu vào cột vehicle_id
            gps_data["timestamp"],  # Lưu vào cột timestamp
            date_ref_val,           # Lưu vào cột date_ref
            gps_data["latitude"],   
            gps_data["longitude"],  
            gps_data["speed"],
            gps_data["longitude"],  # X cho geom
            gps_data["latitude"]    # Y cho geom
        ))

        new_id = cur.fetchone()[0]
        conn.commit()
        cur.close()

        # 4. Tạo bản đồ báo cáo
        m = folium.Map(location=[gps_data["latitude"], gps_data["longitude"]], zoom_start=19)
        popup = f"ID: {new_id} - Xe: {gps_data['device_id']} - Tốc độ: {gps_data['speed']}km/h"
        folium.Marker([gps_data["latitude"], gps_data["longitude"]], popup=popup).add_to(m)

        return jsonify({
            "status": "saved",
            "gps_id": new_id,
            "map_html": m._repr_html_(),
            "message": f"Đã lưu thành công bản ghi {new_id} vào bảng!"
        })

    except Exception as e:
        if conn:
            conn.rollback()
        print("LỖI SQL:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=5000, debug=True)