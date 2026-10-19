# AirQuality Guardian - Architecture

## Overview

AirQuality Guardian is a Self-Healing Data Pipeline for Real-Time Air Quality Monitoring using MongoDB and Datadog.

## System Architecture


┌──────────────────────────────────────────────────────────────────────────┐
│                          AIRQUALITY GUARDIAN                             │
│                       Self-Healing Data Pipeline                         │
└──────────────────────────────────────────────────────────────────────────┘
                                     │
                 ┌───────────────────┴───────────────────┐
                 │                                       │
                 ▼                                       ▼
┌────────────────────────────────┐    ┌────────────────────────────────┐
│        DATA PIPELINE            │    │       SELF-HEALING & OPS        │
│           Người 1               │    │           Người 2               │
├────────────────────────────────┤    ├────────────────────────────────┤
│                                │    │                                │
│  1. Data Fetcher                │    │  1. Self-Healing Engine        │
│     - OpenAQ v3 API             │    │     - Retry                    │
│     - Sensor Cache              │    │     - Quarantine               │
│                                │    │     - Replay                   │
│  2. Data Validator              │    │                                │
│     - Schema Validation         │    │  2. MongoDB Integration        │
│     - Parameter Filtering       │    │     - Time Series              │
│     - Value Validation          │    │     - CRUD Operations          │
│                                │    │                                │
│  3. Data Models                 │    │  3. Observability               │
│     - Pydantic                  │    │     - Datadog Dashboard        │
│     - Data Structure            │    │     - Monitors & Alerts        │
│                                │    │     - Logs                      │
└────────────────┬───────────────┘    └────────────────┬───────────────┘
                 │                                      │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                    ┌──────────────────────────────┐
                    │           MongoDB            │
                    │       Time Series Data       │
                    └──────────────┬───────────────┘
                                   │
                                   ▼
                    ┌──────────────────────────────┐
                    │           Datadog            │
                    │   Monitoring & Alerting      │
                    └──────────────────────────────┘



## Data Flow

1. **Fetch**: DataFetcher gọi OpenAQ v3 API để lấy measurements mới nhất
2. **Enrich**: Fetch sensor info riêng để biết parameter name (pm25, o3, etc.)
3. **Validate**: DataValidator kiểm tra schema, filter parameters hợp lệ
4. **Insert**: (Người 2) Insert vào MongoDB Time Series Collection
5. **Monitor**: (Người 2) Gửi metrics và logs lên Datadog

## Self-Healing (Loại A - TRỌNG TÂM)

- **Retry**: Tự retry 3 lần với exponential backoff (30s, 60s, 120s)
- **Quarantine**: Dữ liệu lỗi được lưu riêng, không mất
- **Replay**: Khi hệ thống hồi phục, tự động replay dữ liệu từ quarantine
- **Idempotency**: Upsert theo event_id, không duplicate

## Data Quality Monitoring (Loại B - PHỤ)

- AQI Calculation theo chuẩn EPA
- Alert khi AQI vượt ngưỡng
- Auto-resolve khi AQI ổn định

## Tech Stack

- **Python 3.11+**
- **MongoDB** (Time Series Collections)
- **Datadog** (Dashboard, Monitors, Logs)
- **OpenAQ v3 API** (Data source)
- **Docker** (Deployment)