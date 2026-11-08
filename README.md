# AirQuality Guardian

> **Self-Healing Data Pipeline cho Real-Time Air Quality Monitoring**  
> Dự án tham gia Hackathon — Tích hợp **MongoDB Atlas (Time Series)** & **Datadog Observability**.

[![CI Pipeline](https://github.com/nguyendinhtanloc-Org/hackathone-airquality-guardian/actions/workflows/ci.yml/badge.svg)](https://github.com/nguyendinhtanloc-Org/hackathone-airquality-guardian/actions)
![Python Version](https://img.shields.io/badge/Python-3.10%20%7C%203.11-3776AB?logo=python&logoColor=white)
![MongoDB Atlas](https://img.shields.io/badge/MongoDB%20Atlas-Time%20Series-47A248?logo=mongodb&logoColor=white)
![Datadog Observability](https://img.shields.io/badge/Datadog-Observability-632CA6?logo=datadog&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.10-017CEE?logo=apacheairflow&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-Enabled-2496ED?logo=docker&logoColor=white)
![Test Suite](https://img.shields.io/badge/Tests-236%20Passed-brightgreen)
![License](https://img.shields.io/badge/License-MIT-blue.svg)

---

## 1. Giới thiệu

Hệ thống giám sát chất lượng không khí tại Hà Nội (Trạm quan trắc Giải Phóng - ID `4946813`) sử dụng dữ liệu từ OpenAQ v3 với khả năng **tự phát hiện, cách ly và tự phục hồi (Self-Healing)** khi xảy ra sự cố API sập, timeout mạng, hoặc cảm biến gửi dữ liệu dị thường.

### Điểm nổi bật
- **Self-Healing Pipeline**: Tự động Retry với hàm mũ (10s -> 20s -> 40s), cách ly (Quarantine) vào MongoDB khi cạn retry, và Replay phục hồi an toàn.
- **Deduplication Tuyệt đối**: Cơ chế **Marker-First Dedup (SHA-256)** bảo đảm tính Idempotent — không bao giờ trùng lặp bản ghi kể cả khi replay hay crash giữa chừng.
- **MongoDB Time Series**: Lưu trữ tối ưu với Time Series Collection (`timestamp`, `metadata.location_id`), tích hợp TTL tự dọn dẹp.
- **Datadog Full Observability**: Dashboard 12 widgets theo dõi throughput, độ trễ, logs sự cố có cấu trúc, và 3 Monitors cảnh báo tự động.
- **236/236 Tests Passed**: Bao phủ toàn bộ Unit test, Integration test, E2E và self-healing scenario.

---

## 2. Kiến trúc Hệ thống & Luồng Dữ liệu (System Architecture)

```mermaid
flowchart TD
    subgraph DataSources["1. Data Sources & Simulation"]
        A1["OpenAQ API v3<br/>(Station ID: 4946813 - Hanoi)"]
        A2["Mock OpenAQ Server<br/>(:8000 - Fault Injection: 500, 401, Timeout)"]
        A3["Scenario Simulator<br/>(11 Real-World Chaos Modes)"]
    end

    subgraph Orchestration["2. Orchestration & Ingestion"]
        B1["Apache Airflow DAG<br/>(airquality_guardian - 5 min schedule)"]
        B2["CLI Scheduler<br/>(python -m src.main --loop)"]
        B3["DataFetcher<br/>(Async HTTPX + Sensor Cache)"]
    end

    subgraph Processing["3. Validation & Enrichment"]
        C1["DataValidator<br/>(EPA Bounds, Filter NaN/Inf/Negative)"]
        C2["AQICalculator<br/>(EPA Standards: PM2.5, PM10, O3, NO2, CO, SO2)"]
    end

    subgraph SelfHealing["4. 3-Tier Self-Healing Engine"]
        D1["Tier 1: Exponential Backoff<br/>(10s -> 20s -> 40s, Max 3 Retries)"]
        D2["Tier 2: Quarantine Isolation<br/>(Store in quarantine_events, Mark Degraded)"]
        D3["Tier 3: Replay Engine<br/>(CAS Claim Lease 600s, Max 5 Attempts)"]
    end

    subgraph MongoAtlas["5. MongoDB Atlas Persistence"]
        E1[("air_quality_dedup<br/>Unique Gate: SHA-256 Marker")]
        E2[("air_quality<br/>Time Series Collection + Partial TTL")]
        E3[("quarantine_events<br/>State Machine: pending/in_flight/replayed")]
        E4[("pipeline_status<br/>Health Check: _id='current'")]
    end

    subgraph DatadogObs["6. Datadog Observability"]
        F1["Datadog Dashboard<br/>(12 Widgets: AQI Gauge, Throughput, Retries)"]
        F2["Automated Monitors<br/>(Degraded, Quarantine Overflow, AQI Unhealthy)"]
        F3["Structured JSON Logs<br/>(Incident Diagnostics & Error Context)"]
    end

    subgraph Interfaces["7. User & Operator Interfaces"]
        G1["Airflow Web UI<br/>(:8080)"]
        G2["Datadog Cloud UI"]
        G3["CLI Report<br/>(--report)"]
    end

    %% Flow Connections
    A1 -->|Fetch measurements| B3
    A2 -->|Mock responses| B3
    A3 -.->|Simulate faults| B3

    B1 -->|Scheduled trigger| B3
    B2 -->|Continuous loop| B3

    B3 -->|Raw API response| C1
    B3 -.->|Network/5xx failure| D1
    D1 -->|Retry attempt| B3
    D1 -->|Exhausted retries / 4xx| D2

    C1 -->|Validated readings| C2
    C1 -.->|Unrecoverable bad record| D2

    C2 -->|Enriched document with AQI| E1
    E1 -->|New marker inserted| E2
    E1 -.->|Duplicate hash detected| DROP["Dedup Gate: Dropped (Idempotent)"]

    D2 -->|Isolate failed payload| E3
    E3 -->|Pending records| D3
    D3 -->|Replay through pipeline| B3

    B3 -.->|Pipeline status update| E4
    B3 -.->|Telemetry & logs| DatadogObs

    F2 -.->|Native Auto-Resolve| F1
    DatadogObs --> G2
    B1 --> G1
    E2 -.-> G3
    E4 -.-> G3
```

---

## 3. Tech Stack

- **Ngôn ngữ & Runtime**: Python 3.10+ / 3.11
- **Cơ sở dữ liệu**: MongoDB Atlas (Time Series Collections)
- **Hệ thống giám sát (Observability)**: Datadog (Dashboard, Monitors, Structured Logs, Metrics)
- **Lập lịch tác vụ (Orchestration)**: Apache Airflow 2.10
- **Nguồn dữ liệu**: OpenAQ API v3
- **Đóng gói & Triển khai**: Docker, Docker Compose, Cloud (Render / Railway / Fly.io)

---

## 4. Cài đặt & Chạy Local

### 4.1. Clone và cài đặt môi trường
```bash
git clone https://github.com/nguyendinhtanloc-Org/hackathone-airquality-guardian.git
cd hackathone-airquality-guardian

# Tạo venv
python -m venv venv
# Kích hoạt venv:
# Trên Windows:
.\venv\Scripts\activate
# Trên Linux/Mac:
source venv/bin/activate

# Cài đặt thư viện
pip install -r requirements.txt
```

### 4.2. Cấu hình biến môi trường
Tạo file `.env` từ `.env.example`:
```bash
cp .env.example .env
```
Điền các giá trị:
- `MONGODB_URI`: Chuỗi kết nối MongoDB Atlas
- `OPENAQ_API_KEY`: API Key từ OpenAQ
- `DATADOG_API_KEY`, `DATADOG_APP_KEY`, `DATADOG_SITE`: Thông tin tài khoản Datadog

### 4.3. Các lệnh thực thi chính

```bash
# 1. Chạy 1 chu kỳ bình thường (Fetch -> Validate -> Lưu MongoDB -> Gửi Datadog)
python -m src.main

# 2. Chạy liên tục theo chu kỳ (mỗi 5 phút) - Lệnh dùng để Deploy Cloud
python -m src.main --loop

# 3. Phục hồi và nạp lại dữ liệu từ vùng cách ly (Replay Quarantine)
python -m src.main --replay --replay-limit 100

# 4. Xuất báo cáo nhanh chất lượng không khí hiện tại và lịch sử (Read-only CLI)
python -m src.main --report

# 5. Khởi tạo Dashboard & Monitors lên tài khoản Datadog
python scripts/apply_dashboard.py
python scripts/apply_datadog.py

# 6. Chạy Simulator giả lập 11 kịch bản sự cố để Demo
python scripts/simulate.py
```

### 4.4. Chạy kiểm thử (Test Suite)
```bash
python -m pytest -q
# Kết quả: 236 passed
```

---

## 5. Triển khai Cloud (Deployment)

Chi tiết quy trình triển khai chạy 24/7 lên nền tảng Cloud (Render / Railway / Fly.io) có trong [docs/DEPLOY.md](docs/DEPLOY.md):
- **Start Command**: `python -m src.main --loop`
- **Tự động khởi tạo index**: Pipeline tự động kiểm tra và khởi tạo toàn bộ index (Time Series, Dedup Unique, TTL, Quarantine) khi khởi động.

---

## 6. Tài liệu Chi tiết

- [Kiến trúc Tự phục hồi (Self-Healing Architecture)](docs/SELF_HEALING.md)
- [Mô hình Dữ liệu MongoDB (Data Models & Schemas)](docs/DATA_MODEL.md)
- [Cấu hình Giám sát Datadog (Dashboard & Monitors)](docs/DATADOG.md)
- [Hướng dẫn Triển khai Cloud (Deployment Runbook)](docs/DEPLOY.md)
- [Kịch bản Trình diễn Giám khảo (Demo Runbook)](docs/DEMO.md)
- [Bảng Nhiệm vụ & Tiến độ (Timeline & Tasks)](docs/timeline.md)

---

## 7. Phân công Nhóm

- **Người 1 (Data Pipeline & Runtime)**: OpenAQ Fetcher, Data Validator (EPA ranges), Pipeline Engine, Scheduler (`--loop`), CLI Replay, Dedup SHA-256 Gate, CI/CD E2E tests.
- **Người 2 (Data Platform & Observability & Ops)**: MongoDB Time Series Collections, Datadog Dashboard & Monitors templates, Quarantine Concurrency Management, CLI Report, Deployment & Docs.

---

## 8. License
MIT License
