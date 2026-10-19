# AirQuality Guardian

> Self-Healing Data Pipeline cho Real-Time Air Quality Monitoring

## Giới thiệu

Hệ thống giám sát chất lượng không khí tại Hà Nội sử dụng dữ liệu từ OpenAQ với khả năng tự phát hiện và tự phục hồi khi xảy ra sự cố.

## Điểm nổi bật

- **Self-Healing Pipeline**: Tự retry, quarantine, replay khi có lỗi
- **Real-time Data**: Dữ liệu cập nhật mỗi 5 phút từ OpenAQ v3
- **6 Parameters**: PM2.5, PM10, O3, NO2, CO, SO2
- **Verified**: Test thành công với location 4946813 (Hà Nội)

## Tech Stack

- Python 3.11+
- MongoDB (Time Series Collections)
- Datadog (Dashboard, Monitors, Logs)
- OpenAQ v3 API
- Docker

## Setup

### 1. Clone repo
```bash
git clone https://github.com/YOUR_USERNAME/airquality-guardian.git
cd airquality-guardian
```

### 2. Tạo virtual environment
```bash
python3 -m venv venv
source venv/bin/activate  # Mac/Linux
# venv\Scripts\activate     # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Setup environment
```bash
cp .env.example .env
# Sửa .env với API key thật
```

### 5. Chạy
```bash
# Run once: fetch OpenAQ, save records to MongoDB Atlas, and send metrics/logs
# to Datadog when DATADOG_API_KEY is configured.
python -m src.main

# Docker
docker-compose up
```

One live run writes the fetched measurements to the configured MongoDB
`air_quality` collection. Use it once to verify the integration; don't run it
repeatedly just to view the same data.

#### Where to see the live result

- **Terminal:** the run summary shows fetched/valid/inserted/failed counts. Each
  inserted document is printed with its timestamp, measurement, and AQI.
- **MongoDB Atlas:** open **Database → Browse Collections → airquality →
  air_quality**. Filter by `{"metadata.location_id": 4946813}`, then sort by
  `timestamp` descending. `MONGODB_URI` should include the database name, or set
  `MONGODB_DATABASE` in `.env`.
- **Datadog metrics:** open **Metrics → Explorer** and query
  `pipeline.records.inserted` (or `pipeline.records.valid`); filter by
  `service:airquality-pipeline` and `env:dev`.
- **Datadog logs:** open **Logs → Explorer** and search
  `service:airquality-pipeline "AirQuality pipeline run completed"`.

Metrics can take a short time to appear in Datadog after ingestion. The
application-key setting is not needed for sending pipeline metrics/logs; it is
used for monitor management.

### Chạy toàn bộ test theo thứ tự

```bash
python -m pytest -q
```

Pytest chạy test tuần tự theo mặc định; mỗi unit test vẫn độc lập và không dựa
vào trạng thái do test trước tạo ra. Các flow nhiều thành phần nằm trong
`tests/integration/`. Không dùng `-n` nếu muốn giữ chạy tuần tự.

## MongoDB

Set `MONGODB_URI` and `MONGODB_DATABASE` in `.env`. The client loads these
settings when initialized, and creates indexes on `timestamp` and
`metadata.location_id` for the `air_quality` collection.

```python
from src.database.mongodb import MongoDBClient

database = MongoDBClient()
try:
    documents = database.query_air_quality(
        {"metadata.location_id": 4946813},
        limit=100,
    )
finally:
    database.close()
```

The client uses the `air_quality`, `quarantine_events`, and `pipeline_status`
collections. `aggregate_aqi()` summarizes each location's highest pollutant
AQI per record; its optional filter accepts a MongoDB query such as a timestamp
range.

## Datadog

Set `DATADOG_API_KEY`, `DATADOG_APP_KEY`, and optionally `DATADOG_SITE` in
`.env`. Metrics and logs use the API key; creating monitors requires both keys.
The dashboard and monitor request templates are in `config/datadog/`. They can
be imported or submitted to the Datadog API after reviewing their queries and
thresholds. Tests mock the API and never send data to a Datadog account.

`Pipeline.run_once()` runs fetch → validate → MongoDB persistence. It expects a
configured `DataFetcher`, `DataValidator`, and `MongoDBClient`.

## Cấu trúc project

airquality-guardian/
├── src/
│ ├── fetcher/ # Data fetching (Người 1)
│ ├── validation/ # Data validation (Người 1)
│ ├── self_healing/ # Self-healing engine (Người 2)
│ ├── models/ # Data models (Người 1)
│ ├── database/ # MongoDB operations (Người 2)
│ ├── datadog/ # Datadog integration (Người 2)
│ ├── api/ # API endpoints (Future)
│ └── main.py # Entry point (Người 1)
├── tests/
│ ├── unit/ # Unit tests
│ └── test.py # Debug script
├── docs/
│ └── ARCHITECTURE.md
├── Dockerfile
└── docker-compose.yml


## Data Source

- **OpenAQ v3 API**: https://openaq.org/
- **Location**: 4946813 - Số 1 đường Giải Phóng, Hà Nội
- **Update frequency**: Mỗi 5 phút
- **Parameters**: pm25, pm10, o3, no2, co, so2

## Team

- **Người 1** (Data Pipeline): Fetcher, Validator, Models
- **Người 2** (Self-Healing + Ops): Self-Healing Engine, MongoDB, Datadog

## License

MIT License