# DEPLOY RUNBOOK (AirQuality Guardian)

> Viết 27/10 — dành cho người thực hiện bước deploy (Người 1 hoặc Người 2).
> Toàn bộ code đã xong + 131 test pass. Deploy là bước cơ học: tạo service,
> paste env, verify. ~1 giờ.

## 0. Trước khi bắt đầu

```bash
git pull --rebase origin data-pipeline
python -m pytest tests/ -q   # phải thấy "131 passed"
```

## 1. Thông tin cần (hỏi người có tài khoản)

- Nền tảng: **Railway / Fly.io / Render** (bất kỳ, chọn 1)
- `MONGODB_URI` Atlas (cluster0...mongodb.net + db name)
- `DATADOG_API_KEY`, `DATADOG_APP_KEY`, `DATADOG_SITE` (us.datadoghq.com)
- `OPENAQ_API_KEY`

## 2. Cấu hình service (cùng trên mọi nền tảng)

| Item | Giá trị |
|------|---------|
| Start command | `python -m src.main --loop` |
| Python version | 3.10+ |
| Healthcheck | không cần (pipeline không chạy web server); verify thủ công dưới đây |
| Env | bảng dưới |

### Bảng environment variables

| Var | Giá trị | Ghi chú |
|-----|---------|---------|
| `MONGODB_URI` | (uri Atlas) | phải chứa db name hoặc set `MONGODB_DATABASE` |
| `MONGODB_DATABASE` | `airquality` | nếu URI chưa có db name |
| `OPENAQ_API_KEY` | (key) | |
| `OPENAQ_API_URL` | `https://api.openaq.org/v3` | |
| `DATADOG_API_KEY` / `DATADOG_APP_KEY` / `DATADOG_SITE` | (keys) | để trống = pipeline vẫn chạy, chỉ bỏ telemetry |
| `FETCH_INTERVAL_SECONDS` | `300` | 5 phút/cycle |
| `MAX_RETRIES` | `3` | |
| `RETRY_BACKOFF_SECONDS` | `10` | tổng backoff 70s < chu kỳ 300s |
| `MAX_REPLAY_ATTEMPTS` | `5` | trần replay, hết → record `failed` |
| `QUARANTINE_CLAIM_LEASE_SECONDS` | `600` | |
| `QUARANTINE_RETENTION_DAYS` | `7` | đọc lúc runtime, .env thắng default |
| `DATA_RETENTION_DAYS` | `30` | TTL index air_quality (0 = giữ mãi) |
| `LOG_RETENTION_DAYS` | `14` | |
| `LOG_LEVEL` | `INFO` | |

> Pipeline tự tạo index (dedup unique, TTL timestamp, claim/lease, expires_at)
> khi khởi động — không cần chạy script setup tay.

## 3. Verify sau khi deploy (bắt buộc)

1. **Log**: "Scheduler starting: location_id=4946813 interval=300s"
2. **~5 phút sau**: Atlas collection `air_quality` có document mới (timestamp hiện tại), `pipeline_status` document `_id:"current"` có `status:"running"` + `last_fetch` mới
3. **Datadog** (nếu có key): metric `pipeline.records.inserted` xuất hiện
4. **Restart service**: log phải có `"Scheduler stopped after N cycle(s)"` + Atlas `pipeline_status.status == "stopped"` (graceful shutdown 2.3)

Mất 5 phút chờ 1 cycle → tổng verify ~10 phút.

## 4. KỊCH BẢN FALLBACK: không kịp deploy cloud

**Demo KHÔNG cần cloud.** Cloud chỉ thêm "chạy 24/7"; mọi cảnh demo đều chạy từ local:

```bash
# Trên máy demo (có .env hợp lệ):
python -m src.main          # 1 cycle: fetch OpenAQ thật → insert Atlas thật → Datadog thật
python -m src.main --replay # replay record quarantine
```

Các cảnh demo + lệnh tương ứng:

| Cảnh demo | Cách chứng minh |
|-----------|----------------|
| Pipeline chạy thật | `python -m src.main` → log "MongoDB document: ...", Atlas có record mới |
| Self-healing (OpenAQ 5xx) | chặn mạng/OpenAQ lỗi → log "Fetch failed, retrying" ×3 → degraded + record vào `quarantine_events` |
| Replay | `python -m src.main --replay` → "Replay finished: ... succeeded=1" |
| Pipeline status | query Atlas: `db.pipeline_status.find({_id:"current"})` |
| Datadog | dashboard/metric thật (nếu 2.11 apply được) |

Người cầm demo chỉ cần 1 máy + `venv` + `.env` + mạng. Không cần cloud.

## 5. Ghi chú cho người nhận

- `docs/timeline.md` mục 2.8 có ghi quy ước "A (QuarantineManager) là nguồn ghi
  lifecycle duy nhất; API B (mark_*) chỉ read/inspect" — chưa sync với
  tác giả B (PR #4). Nếu demo cần dùng `mark_quarantine_*`, dừng lại và hỏi
  Người 1 trước, đừng gọi mark_* để đổi status.
- Nếu thêm service: KHÔNG chạy 2 instance `--loop` cùng 1 collection
  quarantine — CAS claim guard (2.8) đã bảo vệ, nhưng chạy 1 instance là đủ.
