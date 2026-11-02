# MongoDB Data Models & Schema Design (AirQuality Guardian)

## 1. Kiến trúc Lưu trữ Dữ liệu trên MongoDB Atlas
Hệ thống sử dụng **MongoDB Atlas** với mô hình phân tách trách nhiệm tối ưu cho ứng dụng IoT / Dữ liệu chuỗi thời gian (Time Series):

1. **`air_quality`**: Time Series Collection lưu trữ quan trắc môi trường.
2. **`air_quality_dedup`**: Standard Collection lưu Dedup Marker với Index Unique để bảo đảm tính Idempotent.
3. **`quarantine_events`**: Collection quản lý vòng đời dữ liệu bị cách ly chờ tự phục hồi (Self-Healing).
4. **`pipeline_status`**: Collection giám sát trạng thái sức khỏe của pipeline (`_id: "current"`).

---

## 2. Chi tiết các Collections

### 2.1. `air_quality` (Time Series Collection)
- **Tùy chọn tạo collection**:
  - `timeField`: `"timestamp"`
  - `metaField`: `"metadata.location_id"`
  - `granularity`: `"seconds"`
- **Mẫu Document**:
  ```json
  {
    "_id": {"$oid": "..."},
    "event_id": "9b1deb4d-3b7d-4bad-9bdd-2b0d7b3dcb6d",
    "station_id": 4946813,
    "timestamp": {"$date": "2026-10-31T10:00:00Z"},
    "metadata": {
      "location_id": 4946813,
      "location_name": "Số 1 Giải Phóng, Hà Nội"
    },
    "measurements": {
      "pm25": 38.5,
      "pm25_unit": "µg/m³",
      "pm10": 65.0,
      "pm10_unit": "µg/m³",
      "no2": 21.0,
      "no2_unit": "ppm"
    },
    "aqi": {
      "pm25_aqi": 108.0,
      "pm10_aqi": 56.0,
      "primary_pollutant": "pm25"
    },
    "dedup_key": "c3ab8ff13720e8ad9047dd39466b3c8974e592c2fa383d4a3960714caef0c4f2"
  }
  ```
- **Indexes**:
  - `air_quality_ttl_idx`: TTL index trên `timestamp` kèm `partialFilterExpression` trên `metadata.location_id` (tránh crash Code 72 trên MongoDB Atlas khi tạo TTL index cho Time Series).

### 2.2. `air_quality_dedup` (Deduplication Gate)
Vì MongoDB Time Series không hỗ trợ Unique Index, cơ chế **Marker-First Deduplication** được thiết kế:
1. Ghi marker chứa `dedup_key` (SHA-256 từ `metadata + timestamp + measurements + aqi`) vào collection `air_quality_dedup`.
2. Nếu gặp `DuplicateKeyError`: Bỏ qua (idempotent, không ghi đè, không nhân đôi dữ liệu).
3. Ghi dữ liệu vào `air_quality`. Nếu ghi lỗi, rollback marker để cho phép retry.
- **Indexes**:
  - Unique Index: `{ "dedup_key": 1 }` (name: `air_quality_dedup_key_idx`).
  - TTL Index: `{ "created_at": 1 }` khớp với retention của dữ liệu quan trắc.

### 2.3. `quarantine_events` (Hàng đợi Cách ly Sự cố)
Lưu trữ các yêu cầu fetch/process thất bại để phục vụ Replay tự động.
- **Vòng đời State Machine (Quản lý độc quyền bởi QuarantineManager)**:
  `pending` -> `in_flight` (CAS claim với lease timeout 600s) -> `replayed` (thành công) / `failed` (khi chạm trần 5 lần replay).
- **Mẫu Document**:
  ```json
  {
    "_id": {"$oid": "..."},
    "quarantine_id": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
    "data": {"location_id": 4946813},
    "error": "OpenAQ upstream 503 Service Unavailable",
    "status": "pending",
    "replay_attempts": 1,
    "last_replay_error": "Connection timeout",
    "created_at": {"$date": "2026-10-31T09:50:00Z"},
    "in_flight_at": null,
    "expires_at": {"$date": "2026-11-07T09:50:00Z"}
  }
  ```
- **Indexes**:
  - `pending_quarantine_idx`: `[("status", 1), ("created_at", 1)]` (hỗ trợ lấy bản ghi cũ nhất trước).
  - `in_flight_quarantine_idx`: `[("status", 1), ("in_flight_at", 1)]` (hỗ trợ thu hồi lease bị treo do worker crash).
  - TTL Index: `{"expires_at": 1}` tự động dọn dẹp sau 7 ngày.

### 2.4. `pipeline_status` (Trạng thái Vận hành)
Document duy nhất với `_id: "current"`:
```json
{
  "_id": "current",
  "status": "running",
  "last_fetch": {"$date": "2026-10-31T10:05:00Z"},
  "metrics": {
    "pipeline.records.fetched": 6,
    "pipeline.records.valid": 4,
    "pipeline.records.inserted": 4,
    "pipeline.records.skipped_dedup": 2,
    "datavalue.aqi.current": 108.0
  },
  "incidents": []
}
```
Trạng thái có thể nhận: `"running"`, `"degraded"`, `"stopped"`.
