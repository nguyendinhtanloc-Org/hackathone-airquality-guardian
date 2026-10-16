# AirQuality Guardian

Self-Healing Data Pipeline cho Real-Time Air Quality Monitoring

## Giới thiệu

Hệ thống giám sát chất lượng không khí tại Hà Nội sử dụng dữ liệu từ OpenAQ với khả năng tự phát hiện và tự phục hồi khi xảy ra sự cố.

## Điểm nổi bật

- **Self-Healing Pipeline**: Tự động retry, quarantine và replay khi có lỗi
- **Real-time Monitoring**: Dữ liệu được cập nhật mỗi 5 phút
- **MongoDB Time Series**: Lưu trữ và phân tích dữ liệu thời gian thực
- **Datadog Integration**: Dashboard, Monitors và Logs

## Dataset

- **Nguồn**: OpenAQ - Hanoi Air Quality Monitoring Network
- **Location**: Số 1 đường Giải Phóng - phường Bạch Mai - ĐHBK
- **License**: Creative Commons Zero (CC0)

## Tech Stack

- Python 3.10+
- MongoDB (Time Series Collections)
- Datadog (Dashboard, Monitor, Logs)

## Setup

```bash
# Clone repo
git clone https://github.com/YOUR_USERNAME/airquality-guardian.git
cd airquality-guardian

# Tạo virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac
# hoặc
.\venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Copy .env
cp .env.example .env
# Chỉnh sửa .env với thông tin thật

# Run
python src/main.py
```

## Cấu trúc project

airquality-guardian/
├── src/
│ ├── fetcher/ # Data fetching
│ ├── validation/ # Data validation
│ ├── self_healing/ # Self-healing engine
│ ├── api/ # API endpoints
│ └── models/ # Data models
├── tests/
│ ├── unit/ # Unit tests
│ └── integration/ # Integration tests
├── docs/ # Documentation
├── config/ # Configuration
├── data/ # Data files
└── logs/ # Log files

## License

MIT License
