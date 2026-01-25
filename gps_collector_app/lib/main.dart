import 'dart:async';
import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:geolocator/geolocator.dart';
import 'package:http/http.dart' as http;
import 'package:flutter_map/flutter_map.dart';
import 'package:latlong2/latlong.dart';

void main() {
  runApp(const MyApp());
}

class MyApp extends StatelessWidget {
  const MyApp({super.key});

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'GPS Collector',
      theme: ThemeData(primarySwatch: Colors.blue),
      home: const GPSCollectorScreen(),
      debugShowCheckedModeBanner: false,
    );
  }
}

class GPSCollectorScreen extends StatefulWidget {
  const GPSCollectorScreen({super.key});

  @override
  State<GPSCollectorScreen> createState() => _GPSCollectorScreenState();
}

class _GPSCollectorScreenState extends State<GPSCollectorScreen> {
  Position? _currentPosition;
  List<Map<String, dynamic>> _gpsDataList = [];
  Timer? _timer;
  int _remainingSeconds = 0;
  bool _isCollecting = false;
  String _status = "Nhấn nút để bắt đầu thu thập GPS trong 30 giây";

  // Thay bằng IP máy tính chạy Flask
  // Emulator: 10.0.2.2
  // Điện thoại thật: IP LAN (ví dụ: 192.168.1.100)
  final String serverUrl = "http://127.0.0.1:5000/save_gps"; // Emulator

  @override
  void initState() {
    super.initState();
    _checkLocationPermission();
  }

  Future<void> _checkLocationPermission() async {
    bool serviceEnabled = await Geolocator.isLocationServiceEnabled();
    if (!serviceEnabled) {
      setState(() => _status = "Vui lòng bật GPS trên thiết bị");
      return;
    }

    LocationPermission permission = await Geolocator.checkPermission();
    if (permission == LocationPermission.denied) {
      permission = await Geolocator.requestPermission();
      if (permission == LocationPermission.denied) {
        setState(() => _status = "Không có quyền truy cập vị trí");
        return;
      }
    }
  }

  void _startCollecting() {
    if (_isCollecting) return;

    setState(() {
      _isCollecting = true;
      _remainingSeconds = 30;
      _gpsDataList = [];
      _status = "Đang thu thập GPS... (30 giây)";
    });

    // Gửi mỗi 5 giây
    _timer = Timer.periodic(const Duration(seconds: 5), (timer) async {
      if (_remainingSeconds <= 0) {
        _stopCollecting();
        return;
      }

      await _getAndSendGPS();
      setState(() {
        _remainingSeconds -= 5;
        _status = "Đang thu thập... ($_remainingSeconds giây còn lại)";
      });
    });

    // Lấy và gửi ngay lập tức (lần 1)
    _getAndSendGPS();
  }

  Future<void> _getAndSendGPS() async {
    try {
      Position position = await Geolocator.getCurrentPosition(
        desiredAccuracy: LocationAccuracy.high,
      );

      setState(() => _currentPosition = position);

      // Tạo dữ liệu gửi về server
      final gpsData = {
        "gps_id": "G${DateTime.now().millisecondsSinceEpoch}",
        "device_id": "TUAN001",
        "timestamp": DateTime.now().toUtc().toIso8601String(),
        "latitude": position.latitude,
        "longitude": position.longitude,
        "speed": position.speed * 3.6, // km/h
        "accuracy_m": position.accuracy,
        "day_type": DateTime.now().weekday <= 5 ? "weekday" : "weekend",
      };

      _gpsDataList.add(gpsData);

      // Gửi POST đến Flask
      final response = await http.post(
        Uri.parse(serverUrl),
        headers: {"Content-Type": "application/json"},
        body: jsonEncode(gpsData),
      );

      print(
        "Gửi thành công: ${gpsData['gps_id']} - Status: ${response.statusCode}",
      );
    } catch (e) {
      print("Lỗi GPS: $e");
      setState(() => _status = "Lỗi: $e");
    }
  }

  void _stopCollecting() {
    _timer?.cancel();
    setState(() {
      _isCollecting = false;
      _status = "Hoàn thành! Đã gửi ${_gpsDataList.length} điểm GPS";
    });
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(title: const Text("GPS Collector"), centerTitle: true),
      body: Padding(
        padding: const EdgeInsets.all(16.0),
        child: Column(
          crossAxisAlignment: CrossAxisAlignment.stretch,
          children: [
            Card(
              elevation: 4,
              child: Padding(
                padding: const EdgeInsets.all(16.0),
                child: Text(
                  _status,
                  style: const TextStyle(
                    fontSize: 18,
                    fontWeight: FontWeight.bold,
                  ),
                  textAlign: TextAlign.center,
                ),
              ),
            ),
            const SizedBox(height: 20),
            ElevatedButton.icon(
              onPressed: _isCollecting ? null : _startCollecting,
              icon: const Icon(Icons.location_searching),
              label: const Text("BẮT ĐẦU THU THẬP 30 GIÂY"),
              style: ElevatedButton.styleFrom(
                padding: const EdgeInsets.symmetric(vertical: 16),
                textStyle: const TextStyle(fontSize: 18),
              ),
            ),
            const SizedBox(height: 20),
            if (_currentPosition != null) ...[
              const Text(
                "Vị trí hiện tại:",
                style: TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
              ),
              Text("Lat: ${_currentPosition!.latitude.toStringAsFixed(6)}"),
              Text("Lon: ${_currentPosition!.longitude.toStringAsFixed(6)}"),
              Text(
                "Tốc độ: ${(_currentPosition!.speed * 3.6).toStringAsFixed(1)} km/h",
              ),
              Text(
                "Độ chính xác: ${_currentPosition!.accuracy.toStringAsFixed(1)} m",
              ),
            ],
            const SizedBox(height: 20),
            if (_currentPosition != null)
              Expanded(
                child: Card(
                  elevation: 4,
                  clipBehavior: Clip.antiAlias,
                  child: FlutterMap(
                    options: MapOptions(
                      initialCenter: LatLng(
                        _currentPosition!.latitude,
                        _currentPosition!.longitude,
                      ),
                      initialZoom: 15,
                    ),
                    children: [
                      TileLayer(
                        urlTemplate:
                            'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                        userAgentPackageName: 'com.example.app',
                      ),
                      MarkerLayer(
                        markers: [
                          Marker(
                            point: LatLng(
                              _currentPosition!.latitude,
                              _currentPosition!.longitude,
                            ),
                            width: 40,
                            height: 40,
                            child: const Icon(
                              Icons.location_pin,
                              color: Colors.red,
                              size: 40,
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ),
            const SizedBox(height: 20),
            Text(
              "Đã thu thập và gửi: ${_gpsDataList.length} điểm",
              style: const TextStyle(fontSize: 16),
              textAlign: TextAlign.center,
            ),
          ],
        ),
      ),
    );
  }

  @override
  void dispose() {
    _timer?.cancel();
    super.dispose();
  }
}
