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
  List<LatLng> _routePoints = [];
  Timer? _timer;
  StreamSubscription<Position>? _positionStreamSubscription;
  int _remainingSeconds = 0;
  bool _isCollecting = false;
  String _status = "Nhấn nút để bắt đầu thu thập GPS trong 30 giây";

  final MapController _mapController = MapController();

  // Thay bằng IP máy tính chạy Flask
  // Emulator: 10.0.2.2
  // Thiết bị thật: IP của máy tính (vd: 192.168.1.100)
  final String serverUrl = "http://10.0.2.2:5000/save_gps";

  @override
  void initState() {
    super.initState();
    _checkLocationPermission();
    _startLocationTracking(); // Bắt đầu theo dõi vị trí liên tục
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

    if (permission == LocationPermission.deniedForever) {
      setState(() => _status = "Quyền truy cập vị trí bị từ chối vĩnh viễn");
      return;
    }
  }

  // Theo dõi vị trí liên tục
  void _startLocationTracking() {
    const LocationSettings locationSettings = LocationSettings(
      accuracy: LocationAccuracy.high,
      distanceFilter: 0, // Cập nhật mỗi khi có thay đổi
    );

    _positionStreamSubscription =
        Geolocator.getPositionStream(
          locationSettings: locationSettings,
        ).listen((Position position) {
          setState(() {
            _currentPosition = position;
          });

          // Tự động cập nhật bản đồ theo vị trí thực
          try {
            _mapController.move(
              LatLng(position.latitude, position.longitude),
              _mapController.camera.zoom,
            );
          } catch (e) {
            // MapController chưa được khởi tạo
          }

          print("Vị trí cập nhật: ${position.latitude}, ${position.longitude}");
          print("Độ chính xác: ${position.accuracy}m");
        });
  }

  void _startCollecting() {
    if (_isCollecting) return;

    setState(() {
      _isCollecting = true;
      _remainingSeconds = 30;
      _gpsDataList = [];
      _routePoints = [];
      _status = "Đang thu thập GPS... (30 giây)";
    });

    // Lấy và gửi ngay lập tức (lần 1)
    _getAndSendGPS();

    // Gửi mỗi 5 giây
    _timer = Timer.periodic(const Duration(seconds: 5), (timer) async {
      setState(() {
        _remainingSeconds -= 5;
      });

      if (_remainingSeconds <= 0) {
        _stopCollecting();
        return;
      }

      await _getAndSendGPS();
      setState(() {
        _status = "Đang thu thập... ($_remainingSeconds giây còn lại)";
      });
    });
  }

  Future<void> _getAndSendGPS() async {
    try {
      // Sử dụng vị trí hiện tại từ stream nếu có
      Position position =
          _currentPosition ??
          await Geolocator.getCurrentPosition(
            desiredAccuracy: LocationAccuracy.high,
            timeLimit: const Duration(seconds: 10),
          );

      setState(() {
        _currentPosition = position;
        _routePoints.add(LatLng(position.latitude, position.longitude));
      });

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
      try {
        final response = await http
            .post(
              Uri.parse(serverUrl),
              headers: {"Content-Type": "application/json"},
              body: jsonEncode(gpsData),
            )
            .timeout(const Duration(seconds: 5));

        print(
          "Gửi thành công: ${gpsData['gps_id']} - Status: ${response.statusCode}",
        );
      } catch (e) {
        print("Lỗi kết nối server: $e");
        // Vẫn lưu dữ liệu local
      }
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
      appBar: AppBar(
        title: const Text("GPS Collector"),
        centerTitle: true,
        actions: [
          // Nút center vị trí hiện tại
          if (_currentPosition != null)
            IconButton(
              icon: const Icon(Icons.my_location),
              onPressed: () {
                _mapController.move(
                  LatLng(
                    _currentPosition!.latitude,
                    _currentPosition!.longitude,
                  ),
                  16,
                );
              },
              tooltip: "Về vị trí hiện tại",
            ),
        ],
      ),
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
              Card(
                elevation: 2,
                child: Padding(
                  padding: const EdgeInsets.all(12.0),
                  child: Column(
                    crossAxisAlignment: CrossAxisAlignment.start,
                    children: [
                      const Text(
                        "Vị trí hiện tại:",
                        style: TextStyle(
                          fontSize: 16,
                          fontWeight: FontWeight.bold,
                        ),
                      ),
                      const SizedBox(height: 8),
                      Row(
                        children: [
                          Expanded(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  "Lat: ${_currentPosition!.latitude.toStringAsFixed(6)}",
                                ),
                                Text(
                                  "Lon: ${_currentPosition!.longitude.toStringAsFixed(6)}",
                                ),
                              ],
                            ),
                          ),
                          Expanded(
                            child: Column(
                              crossAxisAlignment: CrossAxisAlignment.start,
                              children: [
                                Text(
                                  "Tốc độ: ${(_currentPosition!.speed * 3.6).toStringAsFixed(1)} km/h",
                                ),
                                Text(
                                  "Độ chính xác: ${_currentPosition!.accuracy.toStringAsFixed(1)} m",
                                ),
                              ],
                            ),
                          ),
                        ],
                      ),
                    ],
                  ),
                ),
              ),
            ],
            const SizedBox(height: 20),
            if (_currentPosition != null)
              Expanded(
                child: Card(
                  elevation: 4,
                  clipBehavior: Clip.antiAlias,
                  child: FlutterMap(
                    mapController: _mapController,
                    options: MapOptions(
                      initialCenter: LatLng(
                        _currentPosition!.latitude,
                        _currentPosition!.longitude,
                      ),
                      initialZoom: 17,
                      minZoom: 5,
                      maxZoom: 19,
                      interactionOptions: const InteractionOptions(
                        flags: InteractiveFlag.all,
                      ),
                    ),
                    children: [
                      TileLayer(
                        urlTemplate:
                            'https://tile.openstreetmap.org/{z}/{x}/{y}.png',
                        userAgentPackageName: 'com.example.gps_collector_app',
                        maxZoom: 19,
                      ),
                      // Vẽ đường đi
                      if (_routePoints.length > 1)
                        PolylineLayer(
                          polylines: [
                            Polyline(
                              points: _routePoints,
                              color: Colors.blue,
                              strokeWidth: 4.0,
                            ),
                          ],
                        ),
                      // Các markers
                      MarkerLayer(
                        markers: [
                          // Điểm bắt đầu
                          if (_routePoints.length > 1)
                            Marker(
                              point: _routePoints.first,
                              width: 35,
                              height: 35,
                              child: const Icon(
                                Icons.flag,
                                color: Colors.green,
                                size: 35,
                              ),
                            ),
                          // Vị trí hiện tại
                          Marker(
                            point: LatLng(
                              _currentPosition!.latitude,
                              _currentPosition!.longitude,
                            ),
                            width: 60,
                            height: 60,
                            child: Stack(
                              alignment: Alignment.center,
                              children: [
                                // Vòng tròn độ chính xác
                                Container(
                                  width: 60,
                                  height: 60,
                                  decoration: BoxDecoration(
                                    shape: BoxShape.circle,
                                    color: Colors.blue.withOpacity(0.15),
                                    border: Border.all(
                                      color: Colors.blue.withOpacity(0.6),
                                      width: 2,
                                    ),
                                  ),
                                ),
                                // Vòng trong
                                Container(
                                  width: 20,
                                  height: 20,
                                  decoration: BoxDecoration(
                                    shape: BoxShape.circle,
                                    color: Colors.blue,
                                    border: Border.all(
                                      color: Colors.white,
                                      width: 3,
                                    ),
                                  ),
                                ),
                              ],
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
              style: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
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
    _positionStreamSubscription?.cancel();
    super.dispose();
  }
}
