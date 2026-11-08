# Hướng dẫn Đọc Datadog Dashboard - AirQuality Guardian

## BƯỚC 1: CÁCH CHỈNH THANH ĐIỀU HƯỚNG TRÊN CÙNG

Trước khi quan sát các widget, nhìn lên góc trên bên phải màn hình Datadog:

1. **Khung thời gian (Timepicker)**: Bấm vào và chọn **Past 15 minutes** (hoặc Past 1 hour). Do các script mô phỏng vừa chạy, khung 15 phút sẽ hiển thị biểu đồ trực quan và rõ nét nhất.
2. **Thanh biến lọc (Template Variables)**: Ở hàng trên cùng có 3 biến: `$env`, `$location_id`, `$scenario`.

---

## BƯỚC 2: GIẢI MÃ TỪNG NHÓM WIDGET

### NHÓM 1: TỔNG QUAN HỆ THỐNG (High-Level Overview)

Khu vực nắm bắt trạng thái hoạt động tức thời của hệ thống:

- **Current AQI (Chỉ số AQI hiện tại)**:
  - *Mô tả*: Đo chỉ số AQI tổng hợp cao nhất của trạm Giải Phóng - Hà Nội theo chuẩn US EPA.
  - *Trạng thái*: Khi chạy kịch bản ô nhiễm đột biến (Scenario 9), chỉ số tăng vọt lên ~300 (màu đỏ thẫm - Hazardous); ở chu kỳ vận hành bình thường, chỉ số hồi về mức xanh hoặc vàng (~50-80, Good / Moderate).
  - *Ý nghĩa*: Minh chứng hệ thống tính toán AQI chuẩn xác theo thời gian thực.

- **Pipeline status (1 = Degraded)**:
  - *Mô tả*: Trạng thái sức khỏe pipeline. Bình thường nhận giá trị `0` (màu xanh lá - Healthy); khi xảy ra sự cố API sập hoặc lỗi kết nối DB, giá trị chuyển sang `1` (màu đỏ - Degraded).
  - *Trạng thái*: Tự động chuyển đỏ khi chạy Scenario 10 (sập 503), sau đó trở lại màu xanh khi hệ thống hoàn tất phục hồi.

- **Records inserted (Window)**:
  - *Mô tả*: Tổng số bản ghi sạch đã lưu thành công vào MongoDB Time Series collection trong khung thời gian quan sát.
  - *Trạng thái*: Con số tăng đều theo từng chu kỳ (mỗi chu kỳ hợp lệ nạp từ 2 đến 6 bản ghi).

- **Success rate % (Tỷ lệ xử lý thành công)**:
  - *Mô tả*: Công thức = `100 * (records valid / records fetched)`.
  - *Trạng thái*: Khi gặp Scenario 5 (dữ liệu cảm biến dị thường âm hoặc NaN), tỷ lệ giảm xuống ~33% (vàng/đỏ), sau đó quay về 100% (xanh) ở các chu kỳ sạch.

- **Pending quarantine**:
  - *Mô tả*: Số lượng bản ghi đang cách ly trong MongoDB collection `quarantine_events` chờ xử lý lại.
  - *Trạng thái*: Tăng lên 1 khi sự cố gián đoạn mạng xảy ra, và tự động giảm về 0 ngay sau khi Replay Engine xử lý thành công.

- **Monitors (Trạng thái cảnh báo tự động)**:
  - *Mô tả*: Trạng thái của 3 Datadog Monitors tự động. Hiển thị màu xanh khi hệ thống ổn định (OK) và chuyển đỏ khi có cảnh báo kích hoạt.

---

### NHÓM 2: LƯU LƯỢNG PIPELINE (Pipeline Throughput & Latency)

- **Records per cycle (Biểu đồ lưu lượng dữ liệu)**:
  - Hiển thị các luồng metrics: `fetched` (lấy về), `valid` (hợp lệ), `inserted` (ghi mới), `skipped_dedup` (bỏ qua do trùng lặp), `failed` (bản ghi hỏng).
  - Điểm quan sát: Đỉnh đỏ nhô lên tương ứng với Scenario 5 (4 bản ghi sensor dị thường bị loại bỏ bởi DataValidator).

- **Cycle latency (Độ trễ chu kỳ)**:
  - Đo thời gian từ lúc gửi request tới OpenAQ đến khi hoàn tất ghi dữ liệu vào MongoDB (trung bình dưới 0.5s - 1.5s).

- **Cycles by scenario (Thống kê theo kịch bản)**:
  - Phân loại số chu kỳ đã chạy theo từng scenario: `normal`, `transient_timeout`, `bad_sensor_data`, `pollution_spike`, `outage_then_replay`.

---

### NHÓM 3: KHẢ NĂNG TỰ PHỤC HỒI (Self-Healing Engine - ĐIỂM NHẤN HACKATHON)

Khu vực trọng tâm minh họa kiến trúc Self-Healing 3 tầng:

- **Retries & attempts**:
  - *Mô tả*: Trong điều kiện bình thường, `attempts = 1` (thành công ngay lần đầu).
  - *Trạng thái*: Khi chạy `simulate.py`, biểu đồ ghi nhận đỉnh vọt lên 3 lần retry (Scenario 2: API timeout 2 lần -> hệ thống tự retry lần 3 thành công qua Exponential Backoff).

- **Incidents: retryable vs permanent**:
  - Phân loại lỗi tự động: lỗi tạm thời (Transient Network Error / HTTP 5xx -> tự động Retry) và lỗi vĩnh viễn (HTTP 401 Unauthorized -> không lãng phí tài nguyên retry mà chuyển thẳng vào Quarantine).

- **Quarantine backlog**:
  - Biểu đồ bậc thang: tăng lên khi API gặp sự cố và hạ về 0 khi Replay Engine xử lý dọn dẹp thành công.

---

### NHÓM 4: CHI TIẾT CHẤT Ô NHIỄM (Air Quality Metrics)

- **AQI trend**: Biểu đồ dao động của chỉ số chất lượng không khí tổng hợp theo thời gian.
- **Raw concentrations**: Nồng độ thực tế theo đơn vị vật lý (µg/m³) của từng chất ô nhiễm: PM2.5, PM10, O3, NO2, CO, SO2.
- **AQI by pollutant (Bảng phân tích chi tiết)**: Bảng thống kê chỉ số AQI riêng biệt của từng thông số đo tại chu kỳ cập nhật mới nhất.

---

### NHÓM 5: NHẬT KÝ SỰ CỐ THỜI GIAN THỰC (Logs Stream)

- **Incidents Log Stream**:
  - Lọc riêng các log có `status: error`: thể hiện chi tiết nguyên nhân sự cố (503 Service Unavailable / Timeout), mã sự cố (UUID), số lần đã retry và ngữ cảnh phục hồi.
- **Pipeline runs Log Stream**:
  - Ghi nhận nhật ký mỗi chu kỳ hoàn thành: số lượng bản ghi đọc được, số bản ghi hợp lệ, số bản ghi trùng lặp bị bỏ qua (Deduplication) và tổng thời gian thực thi.
