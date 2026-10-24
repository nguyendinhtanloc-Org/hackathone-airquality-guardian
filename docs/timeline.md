# AirQuality Guardian - Timeline & Phân công công việc

**Đội: 2 người**
**Thời gian làm việc: 5-6 tiếng/ngày mỗi người**
**Mục tiêu: Học hỏi + Deploy lên cloud + Quay video demo**

---

## Tổng quan timeline

```
GIAI ĐOẠN 1: CHUẨN BỊ         │ 17 ngày   ✅ XONG — cả 2 người, 50/50 tests pass
GIAI ĐOẠN 2: BUILD            │ 12 ngày (20-31/10)  🔄 ĐANG LÀM
GIAI ĐOẠN 3: NỘP BÀI          │ 1 ngày (31/10)
GIAI ĐOẠN 4: CHỜ KẾT QUẢ      │ 8 ngày (01-08/11)
```

---

## GIAI ĐOẠN 1: CHUẨN BỊ (Trước ngày 17/10)

> **Mục tiêu:** Hoàn thành 70% code skeleton, không cần tài khoản thật

### Người 1: Data Pipeline

```
NGÀY 1 (Trước 17/10)
═══════════════════════════════════════════════════
□ Tạo GitHub repository
   - Tên repo: airquality-guardian
   - Mô tả: Self-Healing Data Pipeline cho Air Quality Monitoring
   - Public, có README

□ Setup project structure
   mkdir -p src/{fetcher,validation,self_healing,api,models}
   mkdir -p tests/{unit,integration}
   mkdir -p docs config data logs

□ Tạo requirements.txt với các thư viện:
   - pymongo, httpx, pandas, pydantic
   - datadog, loguru, python-dotenv
   - pytest

□ Tạo .env.example:
   MONGODB_URI=mongodb+srv://...
   OPENAQ_API_URL=https://api.openaq.org/v2/latest
   DATADOG_API_KEY=...

□ Tạo .gitignore:
   - .env, __pycache__/, venv/
   - data/, logs/, *.log

□ Commit và push lên branch: data-pipeline

───────────────────────────────────────────────────

NGÀY 2 (Trước 17/10)
═══════════════════════════════════════════════════
□ Viết mock data (mock_openaq_response.json):
   - Lấy format từ OpenAQ docs
   - Tạo 5-10 sample records

□ Viết src/fetcher/fetcher.py:
   - Class DataFetcher
   - Method fetch() với httpx async
   - Retry logic với exponential backoff
   - Timeout handling

□ Viết src/validation/validator.py:
   - Class DataValidator
   - Method validate() - parse và validate data
   - Schema validation
   - Value validation

□ Viết unit tests cho fetcher và validator

□ Commit

───────────────────────────────────────────────────

NGÀY 3 (Trước 17/10)
═══════════════════════════════════════════════════
□ Viết src/models/sensor_data.py:
   - Measurement model
   - SensorData model
   - AirQualityDocument model
   - AQI Levels definition

□ Viết src/main.py:
   - Class Pipeline
   - Method run_once()
   - Method run_loop()
   - Graceful shutdown

□ Test với mock data (không cần API thật)

□ Dockerize:
   - Dockerfile
   - docker-compose.yml

□ Commit

───────────────────────────────────────────────────

NGÀY 4-5 (Trước 17/10)
═══════════════════════════════════════════════════
□ Viết docs/ARCHITECTURE.md:
   - Sơ đồ kiến trúc
   - Data flow
   - Các thành phần

□ Hoàn thiện README.md:
   - Project description
   - Setup instructions
   - Architecture overview

□ Review code, fix bugs

□ Push lên GitHub (branch: data-pipeline)

□ Merge vào main (nếu Người 2 đã xong phần mình)
```

### Người 2: Self-Healing + Ops

```
NGÀY 1 (Trước 17/10)
═══════════════════════════════════════════════════
□ Đọc tài liệu MongoDB Time Series:
   - https://www.mongodb.com/docs/manual/core/timeseries/
   - Ví dụ về Time Series collections

□ Đọc tài liệu Datadog:
   - Dashboard docs
   - Monitors docs
   - Log management docs

□ Setup MongoDB Atlas (free tier):
   - Tạo account
   - Tạo cluster (M0 Sandbox)
   - Tạo database: airquality
   - Tạo collections:
     * air_quality (Time Series)
     * quarantine_events
     * pipeline_status

□ Test kết nối từ local

□ Commit

───────────────────────────────────────────────────

NGÀY 2 (Trước 17/10)
═══════════════════════════════════════════════════
□ Viết src/self_healing/engine.py:
   - Class Incident
   - Class QuarantinedData
   - Class SelfHealingEngine
   - Method handle_error()
   - Method should_retry()
   - Method quarantine_data()
   - Method replay_quarantined()

□ Viết src/self_healing/quarantine.py:
   - Class QuarantineManager
   - Method add_to_quarantine()
   - Method replay()
   - Method cleanup()

□ Viết unit tests cho self-healing

□ Commit

───────────────────────────────────────────────────

NGÀY 3 (Trước 17/10)
═══════════════════════════════════════════════════
□ Viết src/database/mongodb.py:
   - Class MongoDBClient
   - Method insert_air_quality()
   - Method insert_quarantine()
   - Method update_pipeline_status()
   - Method query_air_quality()
   - Method aggregate_aqi()

□ Tạo MongoDB indexes:
   - Index trên timestamp
   - Index trên location_id

□ Viết unit tests cho MongoDB operations

□ Commit

───────────────────────────────────────────────────

NGÀY 4 (Trước 17/10)
═══════════════════════════════════════════════════
☑ Viết src/datadog/integrator.py:
   - Class DatadogIntegrator
   - Method send_metrics()
   - Method send_logs()
   - Method create_monitor()

☑ Tạo Datadog Dashboard template (JSON):
   - Pipeline health panel
   - Self-healing metrics panel
   - AQI overview panel
   - Logs panel

☑ Tạo Datadog Monitors template (JSON):
   - Pipeline degraded monitor
   - Quarantine overflow monitor
   - AQI unhealthy monitor

☑ Unit tests cho Datadog API requests (mock)

☑ Commit

───────────────────────────────────────────────────

NGÀY 5 (Trước 17/10)
═══════════════════════════════════════════════════
☑ Viết src/pipeline.py với run_once() fetch → validate → MongoDB

☑ Viết tests/integration/test_pipeline.py:
   - Test fetcher → validator → MongoDB flow
   - Test self-healing flow

□ Review code của Người 1

☑ Merge branches (data-pipeline đã được merge vào selfhealing)

☑ Integration test (external services được mock)

□ Push lên GitHub (branch: self-healing)
```

---

## GIAI ĐOẠN 2: BUILD (20-31/10) — MVP chạy thật + demo

> **Mục tiêu chung:** Hệ thống chạy end-to-end với MongoDB Atlas + Datadog thật, tự phục hồi khi lỗi, và có video demo.
> **Cách chia:** Mỗi người giữ một "nhánh sở hữu" — code của người này không bị người kia sửa mà không hỏi.

**Trạng thái hiện tại:**

```
GIAI ĐOẠN 1: CHUẨN BỊ    │ ✅ XONG (cả 2 người) — 50/50 tests pass
GIAI ĐOẠN 2: BUILD         │ 🔄 ĐANG LÀM — bắt đầu 20/10
GIAI ĐOẠN 3: NỘP BÀI       │ ○ 31/10
GIAI ĐOẠN 4: CHỜ KẾT QUẢ   │ ○ 01-08/11
```

### Khoảng trống Giai đoạn 2 (đã rà code sau khi merge)

Giai đoạn 1 đã có đủ code skeleton. Giai đoạn 2 **không phải viết mới** — mà là nối các mảnh đã có lại với nhau và chạy thật. 9 mục sau là code đã tồn tại nhưng chưa ai gọi:

| # | Khoảng trống | Hiện trạng |
|---|-------------|-----------|
| 1 | `SelfHealingEngine` không được gọi ở đâu | Class đầy đủ, 0 call site |
| 2 | `QuarantineManager` không lưu vào MongoDB | `__init__(collection=None)` — không ai truyền collection, chạy in-memory |
| 3 | `MongoDBClient.insert_quarantine()` không ai gọi | Method có, 0 call site |
| 4 | `MongoDBClient.update_pipeline_status()` không ai gọi | Method có, 0 call site |
| 5 | `query_air_quality()` / `aggregate_aqi()` không ai gọi | Method có, 0 call site |
| 6 | `DataFetcher.max_retries` đọc từ env nhưng không dùng | Dead config |
| 7 | `main.py` chạy **một lần rồi thoát** — không có scheduler | Giai đoạn 1 plan có `run_loop()`, giờ không có |
| 8 | `create_monitor()` không ai gọi | `monitors.json` là template tĩnh, chưa apply lên Datadog thật |
| 9 | `air_quality` chưa phải Time Series collection | Giai đoạn 1 plan nói là Time Series |

### Nguyên tắc chia: cả hai đều code, chia theo module

```
NGƯỜI 1: 7 hạng mục code — Runtime & Pipeline
NGƯỜI 2: 7 hạng mục — Data Platform & Observability (6 code + 1 docs)
```

### Bảng phân công

| Ngày | NGƯỜI 1 — Runtime & Pipeline | NGƯỜI 2 — Data Platform & Observability |
|------|---------------------------|--------------------------------------|
| **20/10 (T4)** | 2.1 Nối `SelfHealingEngine` vào `Pipeline.run_once()`: lỗi → `handle_error()` → Incident ✅ | 2.11 `scripts/apply_datadog.py`: apply `config/datadog/dashboard.json` + `monitors.json` lên Datadog thật |
| **21/10 (T5)** | 2.2 Retry loop + exponential backoff trong `Pipeline.run_once()` (dùng `engine.should_retry()`, `RETRY_BACKOFF_SECONDS`) ✅ | 2.8 Nối `QuarantineManager` vào MongoDB: truyền `collection` thật, bỏ đường in-memory khỏi luồng chính |
| **22/10 (T6)** | 2.3 Scheduler `run_loop()` chạy mỗi `FETCH_INTERVAL_SECONDS`, graceful shutdown (SIGTERM) | 2.9 `MongoDBClient`: thêm `query_pending_quarantine()`, `mark_quarantine_replayed()` |
| **23/10 (T7)** | 2.4 Vòng đời `pipeline_status`: `running` → `degraded` / `stopped` | 2.10 `air_quality` thành **Time Series collection** trên Atlas + index tuning (`timeField`, `metaField`) |
| **24/10 (T8)** | 2.5 Incident → Datadog log (`status=error`) + metric `pipeline.incidents.count` | 2.12 Monitor runtime: `create_monitor()` được gọi khi có incident, tag theo location, auto-resolve |
| **25/10 (CN)** | 2.6 CLI `python -m src.main --replay` ✅ (đã làm sớm 23/10) | 2.13 CLI `python -m src.main --report`: dùng `aggregate_aqi()` + `query_air_quality()` xuất báo cáo AQI theo location |
| **26/10 (T2)** | 2.7 E2E test + CI/CD + deploy cloud | 2.14 Docs: `DATADOG.md`, `DATA_MODEL.md`, `SELF_HEALING.md` (viết sau khi code xong) |
| **27/10 (T3)** | Verify trên cloud: data chảy vào Atlas, dashboard update | Setup Datadog thật + screenshot + slide |
| **28/10 (T4)** | Test cảnh báo thật: AQI cao → monitor trigger → Slack | Kịch bản + dựng slide, tập trình luyện 1 lần |
| **29/10 (T5)** | Test recovery: tắt network → retry → quarantine → bật lại → replay | Quay video demo (OBS Studio, màn hình cloud + webcam) |
| **30/10 (T6)** | Review toàn bộ code, đọc lại README, kiểm tra docs khớp code | Review docs, rà slide lần cuối |
| **31/10 (T7)** | **NỘP BÀI 23:59 ICT** | **NỘP BÀI 23:59 ICT** |

### Chi tiết hạng mục Người 1 — Runtime & Pipeline

```
2.1 Tích hợp Self-Healing vào Pipeline   ✅ XONG 23/10
   Files: src/pipeline.py, src/fetcher/fetcher.py, src/main.py
   - DataFetcher.fetch() ném exception phân loại thay vì trả None:
     TimeoutError (timeout) / ConnectionError (5xx, lỗi mạng) / AirQualityFetchError (4xx)
   - Pipeline nhận SelfHealingEngine qua constructor (default = SelfHealingEngine())
   - run_once() bọc fetch trong try/except → engine.handle_error() → trả Incident
   - PipelineRunResult thêm field: incident, status ("running" | "degraded")
   - Verify: 7 test trong tests/unit/test_pipeline_self_healing.py

2.1b Hardening edge cases (review 24/10)   ✅ XONG 24/10
   Files: src/pipeline.py, src/validation/validator.py, src/self_healing/engine.py,
          src/main.py, src/database/mongodb.py
   - BUG: insert MongoDB lỗi giữa chừng → exception thoát ra ngoài run_once(),
     scheduler sẽ chết. Sửa: bọc validate+insert trong try/except, trả
     status="degraded", incident component="database".
   - BUG: fetched>0 nhưng inserted==0 vẫn status="running" → mất dữ liệu
     im lặng. Sửa: degraded. Response rỗng vẫn là "running" (location
     đơn giản là không có phép đo).
   - BUG: engine.incidents tích luỹ mãi giữa các vòng chạy → rò RAM và
     update_pipeline_status ghi incidents cũ. Sửa: reset_incidents() mỗi run.
   - BUG: Incident.incident_id luôn None → không truy vết được incident.
     Sửa: sinh uuid4 trong handle_error().
   - BUG: validator nuốt giá trị vô lý (-999, None, inf) và vẫn insert.
     Sửa: MAX_PLAUSIBLE theo từng chất + chặn âm/NaN/inf.
   - BUG: metrics định nghĩa trong nhánh `else` của if datadog → NameError
     khi không cấu hình Datadog. Sửa: đưa ra ngoài if.
   - insert_quarantine() thiếu field của QuarantineManager → sửa cho khớp
     để record ghi bằng hàm này cũng replay được.
   - Verify: 11 test trong tests/unit/test_pipeline_edge_cases.py

2.2 Retry loop + exponential backoff   ✅ XONG 23/10
   File: src/pipeline.py
   - Vòng lặp retry đặt ở Pipeline (KHÔNG ở fetcher) để SelfHealingEngine
     là nơi duy nhất quyết định "retry hay quarantine"
   - engine.should_retry() kiểm tra attempt < max_retries (0-based)
     → MAX_RETRIES=3 nghĩa là 4 lần gọi fetch (1 lần đầu + 3 lần retry)
   - Backoff = RETRY_BACKOFF_SECONDS * 2^attempt → 30s → 60s → 120s
   - main.py: SelfHealingEngine(max_retries=int(os.getenv("MAX_RETRIES", "3")))
   - Xoá fetcher.max_retries (tránh 2 nơi cùng quyết định số lần retry)
   - PipelineRunResult thêm field: attempts
   - Verify: 9 test trong tests/unit/test_pipeline_retry.py

2.3 Scheduler run_loop()
   File: src/main.py
   - Chạy pipeline mỗi FETCH_INTERVAL_SECONDS (mặc định 300)
   - Graceful shutdown: bắt SIGTERM/SIGINT → dừng sau khi vòng hiện tại xong
   - Verify: test giả lập 2 vòng rồi stop → đúng 2 lần chạy

2.4 Vòng đời pipeline_status   ✅ XONG 23/10
   File: src/main.py
   - Sau khi pipeline.run_once() xong, gọi database.update_pipeline_status()
     với: result.status (running/degraded/stopped), metrics={fetch latency,
     records counts, aqi.current}, incidents=[vars(incident), ...].
   - Nếu có incident → status="degraded"; nếu không có → status="running".
   - Xoá file logs cũ, giữ record mới nhất trong MongoDB collection pipeline_status.
   - Verify: 70/70 test vẫn pass; kiểm tra MongoDB aggregation $group
     theo status, last_fetch timestamp.

2.5 Incident lên Datadog
   File: src/main.py
   - Gửi log status="error" cho mỗi incident
   - Thêm metric pipeline.records.failed, pipeline.incidents.count
   - Verify: test main() với fetcher fail → datadog.send_logs được gọi

2.6 CLI --replay   ✅ XONG 23/10
   Files: src/main.py, src/self_healing/quarantine.py, src/self_healing/engine.py
   - argparse: --replay, --replay-limit (mặc định 100), --location-id
   - main(argv) nhận argv để test được; parse_args(None) fallback sang sys.argv[1:]
   - QuarantineManager.replay_async() mới (bản sync replay() giữ nguyên cho
     Người 2) - dùng inspect.isawaitable để hỗ trợ cả processor sync lẫn async
   - Processor kiểm tra result.status == "running" AND records_inserted > 0,
     nếu không thì raise → record giữ status="pending" (không đánh dấu replayed
     khi dữ liệu chưa được lưu lại)
   - QuarantineManager nối vào MongoDB: collection=database.quarantine_events
   - Verify: 12 test trong tests/unit/test_replay_cli.py

2.7 E2E test + CI/CD + deploy
   Files: tests/integration/, .github/workflows/, Dockerfile
   - Kịch bản 1: fetch OK → insert Atlas
   - Kịch bản 2: fetch fail 3 lần → quarantine 1 record → status degraded
   - Kịch bản 3: replay → insert thành công → status running
   - Deploy: chọn 1 trong AWS ECS / GCP Cloud Run / Azure Container Apps (free tier)
   - Verify: pytest pass + pipeline chạy được trên cloud
```

### Chi tiết hạng mục Người 2 — Data Platform & Observability

```
2.11 Script apply Datadog
    File: scripts/apply_datadog.py
    - Đọc config/datadog/dashboard.json → POST /api/v1/dashboard
    - Đọc config/datadog/monitors.json → POST /api/v1/monitor cho từng cái
    - Cần DATADOG_API_KEY + DATADOG_APP_KEY
    - Verify: chạy script → dashboard + 3 monitor xuất hiện trên Datadog UI

2.8 Nối QuarantineManager vào MongoDB   🟡 ĐÃ LÀM 1 PHẦN (23/10)
    File: src/self_healing/quarantine.py + src/main.py
    - ✅ main.py: QuarantineManager(collection=database.quarantine_events)
      (làm trong 2.6 vì --replay không có collection thì vô dùng)
    - ✅ _get_pending_records() + _update_record() đã tự đọc/ghi collection
      → 2.6 chạy được với MongoDB thật
    - ⬜ Còn lại: insert_quarantine() trong MongoDBClient đang thiếu field
      (expires_at, updated_at, replay_attempts) so với QuarantineManager.
      Nên BỎ hẳn insert_quarantine() khỏi MongoDBClient (tránh 2 nơi ghi
      cùng 1 collection với schema khác nhau), hoặc sửa cho khớp.
    - ✅ Giữ đường in-memory cho unit test
    - Verify: test với mock collection → insert_one được gọi

2.9 API quarantine trong MongoDBClient   ⬜ CÓ THỂ KHÔNG CẦN
    File: src/database/mongodb.py
    - query_pending_quarantine(limit) → lấy record status="pending" cũ nhất
    - mark_quarantine_replayed(event_id) / mark_quarantine_failed(...)
    - LƯU Ý: QuarantineManager._get_pending_records() đã tự query collection
      và _update_record() đã tự update → CLI --replay (2.6) dùng
      engine.replay_quarantined() là chạy được, KHÔNG cần 2.9.
      Nếu làm 2.9 thì phải bảo đảm 2 API này và QuarantineManager cho
      cùng kết quả (dễ trùng logic). Ưu tiên bỏ 2.9, dồn effort sang 2.10.

2.10 air_quality thành Time Series collection
    Files: src/database/mongodb.py + script setup Atlas
    - timeField="timestamp", metaField="metadata.location_id"
    - Index tuning cho query theo location + time range
    - Verify: collection type = timeSeries trên Atlas UI

2.12 Monitor runtime
    File: src/datadog/integrator.py
    - create_monitor() được gọi khi có incident (hiện chưa ai gọi)
    - Tag theo location_id, severity theo loại lỗi
    - Auto-resolve khi pipeline healthy lại
    - Verify: test create_monitor được gọi khi incident xảy ra

2.13 CLI --report
    File: src/main.py (argparse) hoặc src/cli/report.py
    - python -m src.main --report
    - Dùng aggregate_aqi() + query_air_quality() → báo cáo AQI theo location
    - Verify: test trả về summary đúng shape

2.14 Docs (3 file)
    Files: docs/DATADOG.md, docs/DATA_MODEL.md, docs/SELF_HEALING.md
    - Viết SAU khi code xong để nội dung khớp thực tế
    - DATADOG.md: 4 widget, 3 monitor, ảnh chụp thật
    - DATA_MODEL.md: schema 3 collection, indexes, truy vấn mẫu
    - SELF_HEALING.md: sơ đồ retry → quarantine → replay, bảng lỗi retryable,
      config, hướng dẫn demo lỗi cho video
```

### Quan hệ phụ thuộc

```
2.6 (Người 1)  ──KHÔNG cần──>  2.9 (Người 2)   QuarantineManager.replay() tự query collection
2.1 (Người 1)  ──cần──>  2.8 (Người 2)   QuarantineManager ghi vào MongoDB
2.10 (Người 2) ──cần──>  2.4 (Người 1)   pipeline_status cũng nên là Time Series
```

Lịch đã sắp đúng thứ tự: 2.8 xong trước 22/10 → 2.6 làm ngày 25/10 không bị chặn.
2.9 không còn là nút thắt → Người 2 dồn effort sang 2.10 (Time Series) và 2.12.

### Điều kiện checkpoint

| Ngày | Checkpoint | Nếu chưa đạt |
|------|-----------|--------------|
| 25/10 | Data chảy vào Atlas + Datadog dashboard có số liệu + `--replay` chạy được | Dùng buffer 26/10, cắt 2.14 docs chi tiết |
| 28/10 | Deploy cloud chạy được | Dùng video demo thay deploy live (thể lệ cho phép "bản demo có thể truy cập") |
| 29/10 | Quay xong video 10 phút | Rút còn 7 phút, bỏ phần AQI alert |
| 30/10 | Slide xong, code review xong | Cắt docs/DATA_MODEL.md chi tiết, giữ bản rút gọn |

### Nguyên tắc phối hợp

```
NHÁNH:
  Người 1: data-pipeline     (sở hữu src/fetcher, src/validation, src/aqi, src/models, src/pipeline.py)
  Người 2: self-healing-ops  (sở hữu src/self_healing, src/database, src/datadog, config/, scripts/, docs/)

GIAO DIỆN GIỮA HAI BÊN (đã chốt, không đổi):
  - Pipeline(fetcher, validator, database, aqi_calculator) + SelfHealingEngine
  - MongoDBClient.insert_air_quality(document) — document có event_id, metadata.location_id,
    timestamp (datetime UTC), measurements, aqi
  - DatadogIntegrator.send_metrics(dict) / send_logs(dict) / create_monitor(dict)
  - Tên metric cố định: pipeline.records.fetched, pipeline.records.valid,
    pipeline.records.inserted, pipeline.records.failed, pipeline.fetch.latency,
    datavalue.aqi.current, pipeline.incidents.count

QUY TẮC:
  - Muốn đổi interface → báo trong chat trước, không tự đổi
  - Sửa file của người kia → nhắn trước
  - Merge vào develop mỗi tối, không merge thẳng vào main
  - Test phải pass (pytest tests/) trước khi push
```

---

## Timeline gốc của Giai đoạn 2 (tham khảo)

```
┌─────────────────────────────────────────────────────────────┐
│                    TUẦN 1: HOÀN THÀNH MVP                     │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 17: LỄ KHỞI ĐỘNG                                      │
├─────────────────────────────────────────────────────────────┤
│ SÁNG    │ Workshop MongoDB (12:00-13:00)                      │
│          │ - Q&A với MongoDB experts                         │
│          │ - Hỏi về Time Series                             │
│                                                         │
│ CHIỀU   │ Workshop Datadog (12:00-13:00)                    │
│          │ - Q&A với Datadog experts                         │
│          │ - Hỏi về Monitors, Logs                        │
│                                                         │
│ TỐI     │ Setup tài khoản thật                              │
│          │ □ MongoDB Atlas (đã có)                          │
│          │ □ Datadog (email work để activate)              │
│          │ □ Test kết nối thật                             │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 18-19: HOÀN THIỆN DATA PIPELINE                       │
├─────────────────────────────────────────────────────────────┤
│ □ Tích hợp fetcher với OpenAQ API thật                    │
│ □ Hoàn thiện retry logic (3 lần, exponential backoff)       │
│ □ Test API với dữ liệu thật                               │
│                                                         │
│ □ Hoàn thiện validator                                    │
│ □ Viết AQI calculation (EPA standard)                      │
│ □ Test với dữ liệu thật từ OpenAQ                        │
│                                                         │
│ □ Kết nối data pipeline:                                  │
│   fetcher → validator → MongoDB                            │
│                                                         │
│ □ Test end-to-end                                         │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 20-21: HOÀN THIỆN SELF-HEALING + MONITORING         │
├─────────────────────────────────────────────────────────────┤
│ □ Test self-healing với mock errors                        │
│ □ Test retry logic                                        │
│ □ Test quarantine và replay                               │
│                                                         │
│ □ Setup Datadog Dashboard thật                           │
│ □ Setup Datadog Monitors thật                            │
│ □ Setup Datadog Logs thật                                │
│                                                         │
│ □ Test monitoring:                                        │
│   - Verify dashboard hiển thị đúng                      │
│   - Verify monitors trigger alert đúng                   │
│   - Verify logs stream đúng                              │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 22-23: MVP HOÀN CHỈNH                                │
├─────────────────────────────────────────────────────────────┤
│ □ Fix bugs còn lại                                       │
│ □ Optimize code                                         │
│ □ Thêm error handling                                    │
│ □ Thêm logging                                          │
│                                                         │
│ □ Test toàn bộ flow:                                    │
│   - Fetch → Validate → MongoDB → Datadog                  │
│   - Self-healing → Retry → Quarantine → Replay            │
│                                                         │
│ □ MVP DEMO:                                             │
│   □ Data chảy vào MongoDB thật                          │
│   □ Dashboard hiển thị thật                              │
│   □ Self-healing hoạt động                              │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 24: MVP CHECKPOINT                                    │
├─────────────────────────────────────────────────────────────┤
│ □ MVP phải CHẠY ĐƯỢC:                                   │
│   □ Fetch dữ liệu từ OpenAQ                             │
│   □ Insert vào MongoDB                                   │
│   □ Dashboard hiển thị metrics                          │
│   □ Self-healing retry khi có lỗi                      │
│                                                         │
│ □ Nếu chưa xong → Buffer ngày 24-25                    │
└─────────────────────────────────────────────────────────────┘
```

### Tuần 2: 25-31/10

```
┌─────────────────────────────────────────────────────────────┐
│                    TUẦN 2: DEPLOY + DEMO                     │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 25-26: DEPLOY LÊN CLOUD                             │
├─────────────────────────────────────────────────────────────┤
│ □ Setup CI/CD:                                           │
│   - GitHub Actions                                       │
│   - Build Docker image                                   │
│   - Deploy lên:                                        │
│     * AWS ECS / GCP Cloud Run / Azure Container Apps     │
│     (chọn 1 trong 3, đều có free tier)                │
│                                                         │
│ □ Setup environment variables trên cloud                  │
│ □ Setup MongoDB connection từ cloud                    │
│ □ Setup Datadog Agent trên cloud (nếu cần)            │
│                                                         │
│ □ Test deployment:                                      │
│   □ Pipeline chạy trên cloud                            │
│   □ Data chảy vào MongoDB Atlas                        │
│   □ Dashboard update real-time                           │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 27-28: SELF-HEALING + ALERTING TEST                 │
├─────────────────────────────────────────────────────────────┤
│ □ Test timeout scenario:                                 │
│   - Tắt network → Hệ thống retry 3 lần                │
│   - Quarantine dữ liệu lỗi                             │
│   - Alert được gửi                                     │
│                                                         │
│ □ Test recovery scenario:                                │
│   - Bật lại network → Hệ thống hồi phục              │
│   - Replay dữ liệu từ quarantine                       │
│   - Alert được resolve                                 │
│                                                         │
│ □ Test AQI alerting:                                    │
│   - Inject dữ liệu AQI cao → Alert trigger            │
│   - Verify Slack notification                          │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 29-30: QUAY DEMO VIDEO                              │
├─────────────────────────────────────────────────────────────┤
│ □ CHUẨN BỊ:                                            │
│   - Viết script/demo (10 phút)                          │
│   - Slide thuyết trình (5-7 slides)                     │
│   - Outline nội dung                                    │
│                                                         │
│ □ QUAY VIDEO:                                           │
│   - Phần 1 (2 phút): Giới thiệu bài toán             │
│   - Phần 2 (3 phút): Pipeline hoạt động bình thường   │
│   - Phần 3 (3 phút): Self-healing demo                 │
│   - Phần 4 (2 phút): Kết quả và metrics               │
│                                                         │
│ □ TOOL QUAY:                                            │
│   - OBS Studio (miễn phí)                              │
│   - Loa và mic đã có                                   │
│   - Quay màn hình cloud + webcam                       │
├─────────────────────────────────────────────────────────────┤
│ NGÀY 31: FINAL REVIEW + NỘP BÀI                         │
├─────────────────────────────────────────────────────────────┤
│ □ REVIEW:                                                │
│   - Đọc lại code                                       │
│   - Đọc lại README                                     │
│   - Đảm bảo đủ thông tin                             │
│                                                         │
│ □ NỘP BÀI (23:59 ICT):                                │
│   □ Repository link                                     │
│   □ README.md                                          │
│   □ Video demo                                         │
│   □ Slide thuyết trình                                 │
│   □ Hướng dẫn cài đặt                                 │
│   □ Khai báo dataset (OpenAQ)                          │
│   □ Khai báo công cụ AI (Claude)                      │
└─────────────────────────────────────────────────────────────┘
```

---

## GIAI ĐOẠN 3: NỘP BÀI (31/10)

### Checklist nộp bài

```
HỒ SƠ NỘP BÀI (theo thể lệ 7.2)
═══════════════════════════════════════════════════════════════

□ Tên dự án và mô tả bài toán
   → "AirQuality Guardian - Self-Healing Data Pipeline"

□ Thông tin đội và thông tin liên hệ
   → Team name, members, contact

□ Kiến trúc giải pháp và ứng dụng
   → docs/ARCHITECTURE.md

□ Kiến trúc dữ liệu và data model trên MongoDB
   → docs/DATA_MODEL.md

□ Kiến trúc observability trên Datadog
   → docs/DATADOG.md

□ Link repository mã nguồn và hướng dẫn cài đặt
   → GitHub repo + README.md

□ README mô tả yêu cầu môi trường, cách triển khai
   → README.md

□ MVP chạy được hoặc bản demo có thể truy cập
   → Cloud URL / Video demo

□ Video demo
   → Video file (10 phút)

□ Slide thuyết trình chung kết
   → PDF / PowerPoint

□ Mô tả sự cố hoặc tình huống vận hành được trình diễn
   → Trong video + docs/INCIDENT_SCENARIO.md

□ Khai báo thành phần tái sử dụng, dataset bên ngoài
   □ Dataset: OpenAQ (CC0 License)
   □ Công cụ AI: Claude (Anthropic)
   □ Libraries: requests, pymongo, etc.
```

---

## GIAI ĐOẠN 4: CHỜ KẾT QUẢ (01-08/11)

```
01-04/11  │ Chấm sơ loại
            │ Giám khảo đánh giá bài nộp

05/11      │ Công bố 8 đội vào chung kết
            │ (Nếu vào top 8 → Chuẩn bị thuyết trình)

08/11      │ Chung kết online (dự kiến)
            │ - Thuyết trình 10 phút
            │ - Q&A với giám khảo
            │ - Công bố kết quả
```

---

## Communication Plan

```
HÀNG NGÀY:
─────────────────────────────────────────────────────────────
SÁNG (9:00):
   Team chat: "Hôm nay làm gì?"

TỐI (21:00):
   Team chat: 
   - "Đã xong: A, B"
   - "Đang làm: C"
   - "Bị stuck: D"

GITHUB:
─────────────────────────────────────────────────────────────
□ Mỗi người làm trên branch riêng:
   - Người 1: data-pipeline
   - Người 2: self-healing-ops

□ Khi xong feature → Tạo PR

□ Team review PR trước khi merge

□ Merge vào main hàng ngày

DOCUMENTATION:
─────────────────────────────────────────────────────────────
□ Shared Google Docs/Notion:
   - Quyết định kiến trúc
   - API contracts
   - Troubleshooting guide

□ Trong code:
   - Comments rõ ràng
   - Docstrings đầy đủ
```

---

## Checklist trước ngày 17/10

```
NGƯỜI 1 CẦN XONG:
─────────────────────────────────────────────────────────────
□ GitHub repo đã tạo
□ Project structure đã setup
□ requirements.txt đã viết
□ src/fetcher/fetcher.py
□ src/validation/validator.py
□ src/models/sensor_data.py
□ src/main.py
□ Mock data đã tạo
□ Docker files đã tạo
□ README.md đã viết
□ Unit tests đã viết
□ Push lên GitHub

NGƯỜI 2 CẦN XONG:
─────────────────────────────────────────────────────────────
□ MongoDB Atlas cluster đã setup
□ src/self_healing/engine.py
□ src/self_healing/quarantine.py
□ src/database/mongodb.py
□ src/datadog/integrator.py
□ Datadog Dashboard template
□ Datadog Monitors template
□ Unit tests đã viết
□ Push lên GitHub

CẢ HAI CẦN XONG:
─────────────────────────────────────────────────────────────
□ Có communication channel (Discord/Slack/Zalo)
□ Có shared docs (Google Docs/Notion)
□ Đã họp lên kế hoạch
□ Phân công rõ ràng
```

---

## Nếu bị stuck?

```
VẤN ĐỀ THƯỜNG GẶP:
─────────────────────────────────────────────────────────────

1. Không kết nối được MongoDB Atlas
   → Kiểm tra IP whitelist
   → Kiểm tra connection string
   → Xem MongoDB Atlas status page

2. Không fetch được OpenAQ API
   → Check rate limit
   → Check API endpoint
   → Thử với Postman/curl

3. Datadog không nhận logs
   → Kiểm tra API key
   → Kiểm tra log format
   → Xem Datadog status page

4. Deployment fail trên cloud
   → Check Docker build logs
   → Check environment variables
   → Check cloud quotas

HỖ TRỢ:
─────────────────────────────────────────────────────────────
□ Workshop 17/10: Hỏi MongoDB & Datadog experts
□ Email: mugvnleaders@gmail.com
□ Discord/Slack: Team channel
```

---

## Liên hệ

```
BAN TỔ CHỨC:
□ Email: mugvnleaders@gmail.com
□ Website: https://mini-hackathon-sep-2026.mugvn.com/
□ Thể lệ: https://mini-hackathon-sep-2026.mugvn.com/terms

DATASET:
□ OpenAQ: https://openaq.org/
□ Docs: https://docs.openaq.org/

DOCUMENTATION:
□ MongoDB Time Series: https://www.mongodb.com/docs/manual/core/timeseries/
□ Datadog: https://docs.datadoghq.com/
```

---

## Ghi chú

```
Ngày cập nhật: [HIỆN TẠI]
Người cập nhật: [TÊN CỦA BẠN]
Phiên bản: 1.0
```

---

**Chúc các bạn hoàn thành tốt dự án! 🚀**
