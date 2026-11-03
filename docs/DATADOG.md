# Hướng dẫn & Kiến trúc Quan sát Datadog (AirQuality Guardian)

## 1. Tổng quan Tích hợp Datadog
Hệ thống **AirQuality Guardian** ứng dụng toàn diện 3 trụ cột quan sát của Datadog:
- **Metrics (Chỉ số)**: Giám sát throughput ingestion, độ trễ, số lượng lỗi, số lượng retry, độ dài hàng đợi cách ly và chỉ số ô nhiễm AQI thời gian thực.
- **Logs (Nhật ký có cấu trúc)**: Truy vết toàn bộ vòng đời pipeline và chi tiết từng incident (mã lỗi, loại lỗi, thành phần lỗi, trạng thái retryable).
- **Monitors & Alerts (Cảnh báo)**: Cảnh báo tự động khi pipeline degraded, hàng đợi quarantine bị tràn, hoặc AQI chạm ngưỡng nguy hại cho sức khỏe.

---

## 2. Dashboard: "AirQuality Guardian - Pipeline Health"
File cấu hình: `config/datadog/dashboard.json`.
Cài đặt tự động bằng script:
```bash
python scripts/apply_dashboard.py
```

### Các nhóm Widget chính:
1. **Pipeline Status & Ingestion Health**:
   - `datavalue.aqi.current`: Đồng hồ đo (Gauge) chỉ số AQI tổng hợp mới nhất từ trạm giám sát.
   - `pipeline.status.degraded`: Cảnh báo trực quan khi pipeline bị giảm hiệu năng.
   - `pipeline.records.fetched`, `pipeline.records.inserted`, `pipeline.records.skipped_dedup`, `pipeline.records.failed`: Biểu đồ Timeseries đo lưu lượng dữ liệu qua từng khâu.
2. **Self-Healing & Fault Tolerance**:
   - `pipeline.attempts`: Số lần thử nghiệm lấy dữ liệu mỗi chu kỳ.
   - `selfhealing.retry.total`: Tổng số lần kích hoạt retry exponential backoff.
   - `selfhealing.quarantine.count`: Số lượng bản ghi lỗi đang nằm trong vùng cách ly chờ replay.
   - `pipeline.incidents.retryable` vs `pipeline.incidents.permanent`: Phân loại bản ghi lỗi mạng/tạm thời vs lỗi cấu trúc/vĩnh viễn.
3. **Pollutant Breakdown (Phân tích chi tiết chất ô nhiễm)**:
   - Đo nồng độ và AQI riêng biệt cho 6 chất: PM2.5, PM10, O3, NO2, CO, SO2.
4. **Live Incident Log Stream**:
   - Bảng log thời gian thực với bộ lọc `service:airquality-pipeline status:error`.

---

## 3. Hệ thống Monitors (Cảnh báo tự động)
File cấu hình: `config/datadog/monitors.json`.
Cài đặt tự động bằng script:
```bash
python scripts/apply_datadog.py
```

### 3 Monitor chuẩn hóa:
1. **AirQuality Guardian: pipeline degraded**
   - **Query**: `sum(last_5m):sum:pipeline.records.failed{*} > 0`
   - **Mục đích**: Bật cảnh báo ngay khi có ít nhất 1 bản ghi bị lỗi trong 5 phút qua.
   - **Cơ chế**: Native auto-resolve khi các chu kỳ tiếp theo đạt 0 lỗi mà không cần can thiệp thủ công.
2. **AirQuality Guardian: quarantine overflow**
   - **Query**: `sum(last_5m):avg:selfhealing.quarantine.count{*} > 50`
   - **Mục đích**: Cảnh báo khi số bản ghi cách ly tích tụ quá lớn (> 50 bản ghi), cho thấy dịch vụ ngoài bị sự cố kéo dài.
3. **AirQuality Guardian: AQI unhealthy**
   - **Query**: `max(last_15m):max:datavalue.aqi.current{*} > 150`
   - **Mục đích**: Cảnh báo ô nhiễm không khí vượt ngưỡng bảo vệ sức khỏe (Unhealthy theo tiêu chuẩn EPA Hoa Kỳ).

---

## 4. Danh mục Metrics được phát hành (Custom Metrics)
| Metric Name | Loại | Ý nghĩa |
|---|---|---|
| `pipeline.records.fetched` | count | Số đo đạc đọc được từ OpenAQ v3 |
| `pipeline.records.valid` | count | Số bản ghi vượt qua kiểm tra ngữ nghĩa và vật lý |
| `pipeline.records.inserted` | count | Số bản ghi ghi mới thành công vào MongoDB Time Series |
| `pipeline.records.skipped_dedup` | count | Số bản ghi bị chặn bởi Dedup Unique Key (tránh trùng dữ liệu) |
| `pipeline.records.failed` | count | Số bản ghi thất bại trong chu kỳ |
| `pipeline.fetch.latency` | gauge | Thời gian phản hồi từ API OpenAQ (giây) |
| `pipeline.incidents.count` | count | Số sự cố phát sinh trong chu kỳ hiện tại |
| `selfhealing.quarantine.count` | gauge | Số bản ghi đang ở trạng thái pending/in_flight trong MongoDB |
| `selfhealing.retry.total` | count | Số lần kích hoạt retry mũ |
| `datavalue.aqi.current` | gauge | AQI cao nhất của trạm hiện tại |
| `datavalue.measurement.<param>` | gauge | Nồng độ đo đạc thực tế của từng chất (`pm25`, `pm10`, v.v.) |
| `datavalue.aqi.<param>` | gauge | Chỉ số AQI riêng rẽ của từng chất |
