# AirQuality Guardian - System Architecture & Data Flow

> **Self-Healing Data Pipeline for Real-Time Air Quality Monitoring**  
> Integrated with **MongoDB Atlas (Time Series Collections)** & **Datadog Observability**.

---

## 1. High-Level Architecture Diagram (Mermaid)

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

## 2. Core Components Description

### 1. Data Ingestion & Enrichment
- **`DataFetcher`**: Gửi request bất đồng bộ tới OpenAQ API v3. Tích hợp bộ đệm (Sensor Cache) lưu trữ thông tin cảm biến, giảm $80\%$ số lượng request thừa.
- **`DataValidator`**: Áp dụng giới hạn vật lý theo chuẩn EPA, loại bỏ dữ liệu âm, NaN, Infinite. Nếu một cảm biến gửi giá trị dị thường, các thông số sạch khác vẫn được giữ lại xử lý.
- **`AQICalculator`**: Chuẩn hóa đơn vị đo (ppm/ppb sang $\mu\text{g/m}^3$) và tính toán chỉ số AQI cho 6 chất ô nhiễm chính: PM2.5, PM10, O3, NO2, CO, SO2.

### 2. 3-Tier Self-Healing Architecture
1. **Tier 1 (Exponential Backoff Retry)**: Khi gặp lỗi mạng, timeout hoặc HTTP 5xx từ API, hệ thống tự động retry với khoảng chờ $10\text{s} \to 20\text{s} \to 40\text{s}$ (tối đa 3 lần).
2. **Tier 2 (Quarantine Storage)**: Khi cạn lượt retry hoặc gặp lỗi 4xx client error, pipeline không dừng (no crash) mà chuyển sang trạng thái `degraded`, lưu toàn bộ payload và stack trace vào `quarantine_events`.
3. **Tier 3 (Idempotent Replay)**: Cơ chế Replay với CAS concurrency lock (lease 600s) và giới hạn trần 5 lần replay, tự động tái xử lý bản ghi khi upstream hoạt động trở lại.

### 3. MongoDB Atlas Schema & Collections
- **`air_quality` (Time Series)**: Cấu hình `timeField: "timestamp"`, `metaField: "metadata.location_id"`, hỗ trợ TTL index kèm `partialFilterExpression`.
- **`air_quality_dedup` (Idempotent Gate)**: Unique Index `{ "dedup_key": 1 }` lưu mã băm SHA-256 từ `(metadata + timestamp + measurements + aqi)`, ngăn chặn nhân đôi bản ghi khi replay.
- **`quarantine_events`**: Quản lý trạng thái cách ly (`pending` -> `in_flight` -> `replayed` / `failed`) với TTL tự xóa sau 7 ngày.
- **`pipeline_status`**: Ghi nhận trạng thái vận hành tức thời (`running`, `degraded`, `stopped`) tại `_id: "current"`.

### 4. Datadog Observability & Alerting
- **Metrics**: Đo lường Throughput (`pipeline.records.fetched`, `inserted`, `skipped_dedup`, `failed`), Latency, Retry counter, và Queue length.
- **Logs**: Ghi nhận có cấu trúc kèm thông tin incident và ddtags.
- **Monitors**: 3 monitors tự động kích hoạt và auto-resolve khi hệ thống tự phục hồi (`pipeline degraded`, `quarantine overflow`, `AQI unhealthy`).