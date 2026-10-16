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
# Local
python -m src.main

# Docker
docker-compose up
```

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