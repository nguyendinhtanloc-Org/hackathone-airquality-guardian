# Kịch bản & Thứ tự Lệnh Quay Video Demo (~5 phút)

> **Dự án**: AirQuality Guardian — Self-Healing Data Pipeline cho Real-Time Air Quality Monitoring  
> **Công nghệ**: Docker Compose, MongoDB Atlas, Datadog Observability, Apache Airflow.

---

## A. Bố trí màn hình trước khi quay

1. **Nửa màn hình bên TRÁI**: Cửa sổ **Terminal WSL** (đang ở thư mục repo `hackathone-airquality-guardian`).
2. **Nửa màn hình bên PHẢI**: Trình duyệt Web mở sẵn 2 tab:
   - **Tab 1: Airflow UI** (`http://localhost:8080`) — Đăng nhập sẵn `admin` / `admin`.
   - **Tab 2: Datadog Dashboard** ("AirQuality Guardian - Pipeline Health") — Chọn thời gian **"Past 15 minutes"**, bật **Auto-refresh**.
3. Bấm **`Windows + Alt + R`** (hoặc dùng OBS / Loom) để bắt đầu quay video.

---

## B. Thứ tự 5 bước Demo chi tiết

---

### BƯỚC 1: Chạy chu kỳ bình thường với dữ liệu thật (Happy Path)

#### 1. Lệnh thực hiện trên Terminal:
```bash
docker compose run --rm pipeline python -m src.main
```

#### 2. Kết quả terminal hiển thị (Expected Output):
```
Starting AirQuality Guardian Pipeline
MongoDB database: airquality | collection: air_quality | ping: {'ok': 1}
Got 6 measurements
Valid: 6, Invalid: 0
Pipeline run complete: location_id=4946813 fetched=6 valid=6 inserted=4 skipped_dedup=2 failed=0 duration_seconds=2.3
Datadog accepted metrics: {'errors': []}
Datadog accepted pipeline log.
```

#### 3. Thuyết minh trong video:
- *"Pipeline kết nối OpenAQ API lấy thành công 6 phép đo thật từ trạm Giải Phóng - Hà Nội."*
- *"Dữ liệu được lọc rác qua `DataValidator`, tính chỉ số AQI chuẩn EPA (PM2.5 AQI = 62 - Trung bình), lưu vào MongoDB Time Series và gửi Datadog."*
- **Chỉ sang Datadog**: *"Ô Current AQI hiển thị mức 62 màu vàng, biểu đồ Throughput ghi nhận dữ liệu sạch."*

---

### BƯỚC 2: Giả lập sự cố API sập 500 & Self-Healing (Retry & Quarantine)

#### 1. Lệnh thực hiện trên Terminal:
```bash
docker compose run --rm pipeline python scripts/simulate.py --scenario 3
```

#### 2. Kết quả terminal hiển thị (Expected Output):
```
[Scenario 3] server_outage
  OpenAQ returned HTTP 503 (server outage)
  Fetch failed (ConnectionError), retrying in 10.0s (attempt 1/3)
  Fetch failed (ConnectionError), retrying in 20.0s (attempt 2/3)
  Fetch failed (ConnectionError), retrying in 40.0s (attempt 3/3)
  -> Status: DEGRADED (pipeline did not crash)
  -> Quarantined into MongoDB quarantine_events (pending: 1)
  -> Telemetry & Incident log emitted to Datadog
```

#### 3. Thuyết minh trong video:
- *"Bây giờ em giả lập sự cố upstream API sập hoàn toàn (lỗi HTTP 500/503).*  
- *"Hệ thống tự động kích hoạt **Tier 1 - Exponential Backoff**: thử lại 3 lần với khoảng cách hàm mũ (10s -> 20s -> 40s)."*
- *"Khi cạn 3 lần retry, pipeline **không hề bị văng lỗi (no crash)** mà chuyển trạng thái sang `degraded` và kích hoạt **Tier 2 - Quarantine**: cách ly toàn bộ yêu cầu lỗi vào MongoDB collection `quarantine_events`."*
- **Chỉ sang Datadog**: *"Metric `quarantine.count` tăng lên 1, xuất hiện incident log màu đỏ và Monitor `pipeline degraded` bật cảnh báo."*

---

### BƯỚC 3: Hồi phục API & Tự động Replay dữ liệu

#### 1. Lệnh thực hiện trên Terminal:
```bash
docker compose run --rm pipeline python scripts/simulate.py --scenario 10
```

#### 2. Kết quả terminal hiển thị (Expected Output):
```
[Scenario 10] outage_then_replay
  -> OpenAQ recovered to normal
  -> Replaying quarantined records through pipeline...
  Replay finished: processed=1 succeeded=1 failed=0
  -> Quarantine queue drained to 0
  -> Datadog monitor auto-resolved
```

#### 3. Thuyết minh trong video:
- *"Khi API phục hồi, hệ thống kích hoạt **Tier 3 - Replay Engine**."*
- *"Replay Engine dùng khóa CAS với lease 600s để quét các bản ghi cách ly nạp lại vào MongoDB. Terminal thông báo: `Replay finished: processed=1 succeeded=1`."*
- *"Nhờ cổng kiểm tra SHA-256 Unique Key, dữ liệu nạp lại được bảo đảm Idempotent tuyệt đối, không bao giờ bị nhân đôi."*
- **Chỉ sang Datadog**: *"Hàng đợi quarantine giảm về 0 và Monitor tự động chuyển xanh (Auto-Resolve) mà không cần can thiệp thủ công."*

---

### BƯỚC 4: Xuất báo cáo AQI tổng hợp (CLI Report)

#### 1. Lệnh thực hiện trên Terminal:
```bash
docker compose run --rm pipeline python -m src.main --report
```

#### 2. Kết quả terminal hiển thị (Expected Output):
```
AQI report - location 4946813

Current (newest reading)
  primary pollutant : pm25
  overall AQI       : 62
  level             : moderate - Trung bình [#FFFF00]

  pollutant      value     aqi   level
  pm25         17.1     62  moderate (µg/m³)

Stored window (avg/min/max over retention period)
  location  avg    min    max  records
   4946813   46.1      1    168  50
```

#### 3. Thuyết minh trong video:
- *"Hệ thống có lệnh CLI Report chạy read-only: sử dụng **MongoDB Aggregation Pipeline** đa tầng để tính toán nhanh AQI hiện tại và thống kê lịch sử (trung bình, giá trị min/max, tổng số record) qua retention period."*

---

### BƯỚC 5: Chạy bộ kiểm thử tự động (Test Suite)

#### 1. Lệnh thực hiện trên Terminal:
```bash
docker compose run --rm pipeline pytest -q
```

#### 2. Kết quả terminal hiển thị (Expected Output):
```
236 passed in 10.45s
```

#### 3. Thuyết minh trong video:
- *"Toàn bộ hệ thống được bảo đảm độ tin cậy tuyệt đối với **236/236 tests passed** (bao gồm Unit tests, Integration tests, E2E và Self-healing scenarios)."*
- *"Em xin chân thành cảm ơn Ban giám khảo đã theo dõi phần demo của nhóm AirQuality Guardian!"*

---

## C. Sau khi quay xong

1. Bấm **`Windows + Alt + R`** để kết thúc quay.
2. File video nằm ở thư mục: `Videos\Captures` (hoặc mở ứng dụng Clipchamp).
3. Tải lên **Google Drive** (chọn *"Bất kỳ ai có liên kết đều có thể xem"*) hoặc **YouTube (chế độ Unlisted)**.
4. Dán link video vào Form nộp bài!
