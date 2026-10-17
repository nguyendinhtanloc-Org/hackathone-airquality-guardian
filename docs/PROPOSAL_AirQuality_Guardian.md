# AirQuality Guardian - Self-Healing Data Pipeline cho Real-Time Air Quality Monitoring

---

## 1. Bài toán

Giám sát chất lượng không khí tại Hà Nội với pipeline có khả năng tự phát hiện và tự phục hồi khi xảy ra sự cố.

```
Nguồn dữ liệu: OpenAQ (https://openaq.org/)
Trạm đo: Hanoi Air Quality Monitoring Network
Vị trí: Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK
Tọa độ: 21.0052, 105.8418

Các thông số:
- O3 (Ozone)
- PM2.5
- PM10
- NO2
- CO
- SO2
```

---

## 2. Điểm khác biệt

```
KHÔNG PHẢI: "Self-Healing cho Air Quality"
MÀ LÀ: "Self-Healing cho DATA PIPELINE"

Self-Healing ở đây là về PIPELINE, không phải về không khí.

Hệ thống có thể:
1. Phát hiện khi API bị timeout
2. Tự retry với exponential backoff
3. Quarantine dữ liệu lỗi
4. Tự replay khi API hồi phục

Còn phần Air Quality (AQI) là DATA QUALITY MONITORING,
giúp theo dõi chất lượng không khí.
```

---

## 3. Dataset

### 3.1 Nguồn dữ liệu

```
OpenAQ - Hà Nội Air Quality Monitoring Network

Website: https://openaq.org/
API: https://api.openaq.org/v2/latest

Dataset license: Creative Commons Zero (CC0) Public Domain Dedication

Dữ liệu đã xác minh:
- Location ID: 4946813
- Location: "Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK"
- Network: "Hanoi Air Quality Monitoring Network"
- Update frequency: Mỗi 5 phút
- Date range: Có dữ liệu 1 ngày trở lên
```

### 3.2 Dữ liệu mẫu

```csv
location_id,location_name,parameter,value,unit,datetimeUtc,datetimeLocal
4946813,Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK,o3,24.75,µg/m³,2026-10-11T00:05:00Z,2026-10-11T07:05:00+07:00
4946813,Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK,o3,23.03,µg/m³,2026-10-11T00:10:00Z,2026-10-11T07:10:00+07:00
```

### 3.3 Các thông số

```
O3 (Ozone):
- Giá trị: 3.6 - 72.69 µg/m³
- Unit: µg/m³
- Update: Mỗi 5 phút

Các thông số khác có sẵn:
- PM2.5 (quan trọng nhất cho VN)
- PM10
- NO2
- CO
- SO2
```

---

## 4. Kiến trúc hệ thống

### 4.1 Sơ đồ tổng quan

```
                    OPEN AQ API
                         |
                         v
              DATA FETCHER (Python)
              - Retry với exponential backoff
              - Timeout handling
                         |
                         v
                 VALIDATION GATE
              - Schema validation
              - Required fields check
              - Data type validation
                         |
           +-------------+-------------+
           |                           |
      [Hợp lệ]                   [Lỗi]
           |                           |
           v                           v
    MONGODB TIME SERIES          SELF-HEALING
           |                      ENGINE
           |                 +-----+-----+
           v                 |     |     |
    ANALYTICS           Retry  Quarantine  Replay
      + AQI                 |
           |                 v
           |         Pipeline Status
           v                 |
      DATADOG           Metrics & Logs
```

### 4.2 Hai loại Self-Healing

```
LOẠI A: Pipeline Self-Healing (TRỌNG TÂM)

Sự cố:
- API timeout
- Response lỗi schema
- Missing required fields

Xử lý:
1. Retry với exponential backoff
2. Quarantine dữ liệu lỗi
3. Tiếp tục xử lý dữ liệu hợp lệ
4. Replay khi hồi phục
5. Alert khi vượt giới hạn retry


LOẠI B: Data Quality Monitoring (PHỤ)

Sự cố:
- AQI vượt ngưỡng
- Giá trị bất thường

Xử lý:
1. Alert theo dõi
2. Tự động đóng alert khi ổn định
```

---

## 5. Self-Healing Engine (Loại A - TRỌNG TÂM)

### 5.1 Retry với Exponential Backoff

```python
def fetch_with_retry(url, max_retries=3):
    for attempt in range(max_retries):
        try:
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                return response.json()
        except requests.exceptions.Timeout:
            log.warning(f"Timeout at attempt {attempt + 1}")
        except requests.exceptions.RequestException as e:
            log.error(f"Request error: {e}")
        
        # Exponential backoff
        if attempt < max_retries - 1:
            wait_time = (2 ** attempt) * 30  # 30s, 60s, 120s
            time.sleep(wait_time)
    
    # All retries failed
    return None
```

### 5.2 Quarantine và Replay

```python
# When validation fails
if not is_valid(data):
    quarantine_collection.insert_one({
        "event_id": generate_uuid(),
        "raw_data": data,
        "error_type": validation_error,
        "retry_count": 0,
        "created_at": datetime.utcnow(),
        "status": "pending"
    })
    log.warning(f"Data quarantined: {validation_error}")

# When API recovers, replay quarantine
def replay_quarantine():
    pending = quarantine_collection.find({"status": "pending"})
    for item in pending:
        if validate(item["raw_data"]):
            insert_to_mongodb(item["raw_data"])
            quarantine_collection.update_one(
                {"_id": item["_id"]},
                {"$set": {"status": "completed"}}
            )
```

### 5.3 Idempotency

```python
# Upsert to prevent duplicate
result = collection.update_one(
    {"event_id": event_id},  # Unique key
    {"$set": data},
    upsert=True
)

# If exists, update; if not, insert
# Never duplicate records
```

---

## 6. AQI Calculation (Loại B - PHỤ)

### 6.1 Tiêu chuẩn AQI

```
Sử dụng tiêu chuẩn AQI của Mỹ (EPA)

Công thức:
AQI = ((Ih - Il) / (BPh - BPl)) * (Cp - BPl) + Il

Trong đó:
- Ih = AQI cao
- Il = AQI thấp
- BPh = Breakpoint cao
- BPl = Breakpoint thấp
- Cp = Concentration
```

### 6.2 Breakpoint cho O3

```
O3 (ppb) - 8-hour average:

AQI 0-50 (Good):       0-54
AQI 51-100 (Moderate): 55-70
AQI 101-150 (Unhealthy for Sensitive): 71-85
AQI 151-200 (Unhealthy): 86-105
AQI 201-300 (Very Unhealthy): 106-200
```

### 6.3 Mức độ AQI

```
AQI Level    | Màu sắc | Nguy hiểm                      | Hành động
-------------|----------|--------------------------------|------------------
0-50         | Xanh     | Tốt                          | Không cần làm gì
51-100       | Vàng    | Trung bình                    | Người nhạy cảm nên giảm hoạt động ngoài trời
101-150      | Cam      | Không tốt cho nhóm nhạy cảm  | Hạn chế ra ngoài trời
151-200      | Đỏ      | Không tốt cho sức khỏe        | Trẻ em, người lớn nên hạn chế ra ngoài trời
201-300      | Tím      | Các vấn đề sức khỏe nghiêm trọng | Mọi người nên ở trong nhà
301-500      | Nâu      | Tình trạng khẩn cấp           | Hạn chế đi ra ngoài, đeo khẩu trang
```

---

## 7. MongoDB Collections

### 7.1 air_quality (Time Series)

```javascript
{
    _id: ObjectId,
    metadata: {
        location_id: 4946813,
        location_name: "Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK",
        city: "Hanoi",
        country: "VN",
        coordinates: {
            latitude: 21.0052,
            longitude: 105.8418
        }
    },
    timestamp: ISODate(),
    measurements: {
        o3: 24.75,
        o3_unit: "µg/m³",
        pm25: null,
        no2: null,
        pm10: null
    },
    aqi: {
        o3_aqi: 35,
        level: "good",
        color: "green"
    },
    event_id: "uuid-v4"  // For idempotency
}
```

### 7.2 quarantine_events

```javascript
{
    _id: ObjectId,
    event_id: "uuid-v4",
    raw_data: { ... },
    error_type: "validation_failed",
    error_message: "Missing required field: o3",
    retry_count: 0,
    created_at: ISODate(),
    status: "pending",  // pending, processing, completed, failed
    resolved_at: null
}
```

### 7.3 pipeline_status

```javascript
{
    _id: "current",
    last_fetch: ISODate(),
    status: "running",  // running, degraded, stopped
    metrics: {
        records_fetched: 150,
        records_valid: 148,
        records_failed: 2,
        retry_count: 5,
        quarantine_count: 2
    },
    incidents: [
        {
            incident_id: "INC-001",
            started_at: ISODate(),
            type: "timeout",
            resolved_at: ISODate(),
            resolution: "auto_recovered",
            retry_attempts: 3
        }
    ]
}
```

### 7.4 expireAfterSeconds

```javascript
// Lưu trữ 30 ngày
db.createCollection("air_quality", {
    timeseries: {
        timeField: "timestamp",
        metaField: "metadata",
        granularity: "minutes"
    },
    expireAfterSeconds: 60 * 60 * 24 * 30  // 30 days
})
```

---

## 8. Datadog Integration

### 8.1 Custom Metrics

```python
# Pipeline metrics
pipeline.records.fetched      # Số bản ghi đã fetch
pipeline.records.valid        # Số bản ghi hợp lệ
pipeline.records.failed       # Số bản ghi lỗi
pipeline.fetch.latency        # Độ trễ khi fetch
pipeline.retry.count         # Số lần retry

# Self-Healing metrics (Loại A)
selfhealing.retry.total       # Tổng số retry
selfhealing.retry.success     # Retry thành công
selfhealing.quarantine.count  # Bản ghi trong quarantine
selfhealing.replay.count     # Bản ghi đã replay
selfhealing.incidents.active # Sự cố đang active

# Data Quality metrics (Loại B)
datavalue.aqi.current         # AQI hiện tại
datavalue.aqi.level          # Mức độ AQI (0-6)
datavalue.alert.active       # Alert đang active
```

### 8.2 Logs

```json
{
    "timestamp": "2026-10-11T07:05:00Z",
    "level": "info",
    "service": "airquality-pipeline",
    "message": "Fetched 10 records from OpenAQ",
    "records_count": 10,
    "valid_count": 9,
    "failed_count": 1
}

{
    "timestamp": "2026-10-11T07:05:30Z",
    "level": "error",
    "service": "airquality-pipeline",
    "message": "API timeout, entering retry",
    "retry_count": 1,
    "error": "Connection timeout",
    "endpoint": "https://api.openaq.org/v2/latest"
}

{
    "timestamp": "2026-10-11T07:06:00Z",
    "level": "warn",
    "service": "airquality-pipeline",
    "message": "Data quarantined",
    "event_id": "uuid-v4",
    "error_type": "validation_failed"
}
```

### 8.3 Monitors

```yaml
Monitor 1: Pipeline Degraded
- Condition: pipeline.retry.count > 10 FOR 5 min
- Severity: Warning
- Action: Slack alert

Monitor 2: Quarantine Overflow
- Condition: selfhealing.quarantine.count > 100
- Severity: Critical
- Action: PagerDuty alert

Monitor 3: AQI Unhealthy
- Condition: datavalue.aqi.current > 150 FOR 5 min
- Severity: Warning
- Action: Slack alert

Monitor 4: AQI Hazardous
- Condition: datavalue.aqi.current > 300 FOR 5 min
- Severity: Critical
- Action: PagerDuty alert
```

### 8.4 Dashboard Panels

```
Panel 1: Pipeline Health
- Line chart: records fetched vs valid vs failed
- Line chart: fetch latency

Panel 2: Self-Healing Metrics (Loại A)
- Number: Active incidents
- Number: Quarantine count
- Line chart: Retry count over time

Panel 3: AQI Overview (Loại B)
- Gauge: Current AQI
- Line chart: AQI trend

Panel 4: Logs
- Log stream: Filter by service=airquality-pipeline
```

---

## 9. Kịch bản Demo (10 phút)

### 9.1 Phút 1-2: Giới thiệu

```
Nội dung:
1. Bài toán: Giám sát chất lượng không khí tại Hà Nội
2. Nguồn: OpenAQ - Hanoi Air Quality Monitoring Network
3. Điểm khác biệt: Self-Healing Data Pipeline

Slide: Kiến trúc hệ thống với Self-Healing Engine
```

### 9.2 Phút 3-4: Pipeline hoạt động bình thường

```
Hành động:
1. Chạy Data Fetcher
2. Hiển thị Dashboard

Kết quả:
- Pipeline running
- Records fetched: 10
- Valid: 9, Failed: 1
- AQI: 35 (Good)
```

### 9.3 Phút 5-7: Self-Healing - Loại A (TRỌNG TÂM)

```
Hành động:
1. Mô phỏng API timeout (tắt network)

Hiển thị:
- Log: "API timeout, entering retry"
- Retry 1: Fail
- Retry 2: Fail
- Retry 3: Fail
- Log: "All retries failed, quarantining data"
- Quarantine count: 1
- Alert: Slack notification

Thời gian: < 1 phút
```

### 9.4 Phút 8-9: Phục hồi tự động

```
Hành động:
1. Bật lại network
2. API hồi phục

Hiển thị:
- Log: "API recovered, replaying quarantine"
- Pipeline tiếp tục fetch
- Replay từ quarantine
- Quarantine count: 0
- Incident auto-resolved

Thời gian: < 1 phút
```

### 9.5 Phút 10: Tổng kết

```
Con số đo được:
- Detection latency: X giây
- Recovery time: X giây
- Retry success rate: X%
- Data completeness: X%

Điểm khác biệt:
- Self-Healing = Pipeline tự phục hồi
- Không phải "Air Quality tự phục hồi"
```

---

## 10. Phạm vi MVP (2 tuần)

### P0 - Bắt buộc

| Ngày | Công việc | Deliverable |
|------|-----------|------------|
| 1-2 | Setup MongoDB Atlas, tạo Time Series | Cluster ready |
| 3-4 | Data Fetcher + Validation + Retry | Pipeline core |
| 5-6 | Self-Healing Engine (Retry, Quarantine, Replay) | Self-healing hoạt động |
| 7 | MongoDB Aggregation + Idempotency | Lưu trữ đúng |

### P1 - Quan trọng

| Ngày | Công việc | Deliverable |
|------|-----------|------------|
| 8-9 | Datadog integration (Dashboard, Monitors, Logs) | Monitoring hoạt động |
| 10-11 | AQI Calculation + Data Quality Alert | Alert hoạt động |
| 12-13 | Test + Demo video | Tất cả chạy được |
| 14 | Buffer + Ôn tập | Sẵn sàng |

---

## 11. Phân công (2 người)

### Người 1: Pipeline và Self-Healing

```
- Data Fetcher
- Validation Gate
- Self-Healing Engine (Loại A)
- Quarantine và Replay
- Idempotency
```

### Người 2: Data và Datadog

```
- MongoDB setup
- AQI Calculation (Loại B)
- Datadog Dashboard
- Monitors và Alerts
- Logs integration
```

---

## 12. Đo lường kết quả

### Metrics cần đo

```python
# Detection latency
detection_latency = end_time - incident_start_time

# Recovery time
recovery_time = recovery_time - incident_start_time

# Retry success rate
retry_success_rate = retry_success / retry_total

# Data completeness
data_completeness = records_valid / records_fetched
```

### Không đặt con số giả định

```
Thay vì: "Self-healing success rate: 95%"

Sẽ đo sau demo:
- Detection latency: 30 giây
- Recovery time: 45 giây
- Retry success rate: 100% (3/3)
- Data completeness: 98%
```

---

## 13. Khai báo trong bài nộp

```
Dataset:
- OpenAQ API (https://openaq.org/)
- Creative Commons Zero (CC0) Public Domain Dedication
- Real-time air quality data from Hanoi Air Quality Monitoring Network

Công cụ AI:
- Claude (Anthropic) - khai báo theo thể lệ

Thư viện/Framework:
- pymongo (MongoDB driver)
- requests (HTTP client)
- pandas (Data processing)
- datadog (Datadog integration)
```

---

## 14. Tại sao dự án này đạt giải

### Sáng tạo (30%)

```
- Self-Healing Pipeline (Loại A) - Điểm khác biệt
- Không chỉ monitoring thông thường
- Cơ chế tự phát hiện và phục hồi pipeline
- Dùng dữ liệu THỰC TỪ OpenAQ
```

### Kỹ thuật (30%)

```
- MongoDB Time Series Collections
- Validation Gate đầy đủ
- Retry với exponential backoff
- Quarantine và Replay
- Idempotency
- Datadog Dashboard + Monitors + Logs
```

### Tác động (30%)

```
- Có ý nghĩa xã hội (chất lượng không khí Hà Nội)
- Có thể mở rộng thành sản phẩm thực tế
- Giúp cộng đồng được thông báo sớm
```

### Trình bày (10%)

```
- Demo rõ ràng với self-healing pipeline
- Dashboard đẹp mắt
- Có con số đo lường thực tế
```

---

## 15. Backup Plan

```
Nếu OpenAQ không khả dụng:
1. Dùng dữ liệu đã download (CSV)
2. Parse CSV thay vì call API
3. Pipeline vẫn hoạt động
4. Self-healing vẫn test được

Nếu MongoDB Atlas không khả dụng:
1. Dùng local MongoDB
2. Cấu hình tương tự
3. Demo vẫn chạy được
```

