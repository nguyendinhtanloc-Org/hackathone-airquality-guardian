# Self-Healing Demo — Runbook (Airflow + Mock OpenAQ, local)

Toàn bộ chạy trên 1 máy (Docker Desktop). Atlas + Datadog là 2 dịch vụ
cloud duy nhất (free tier cuộc thi).

## 1. Khởi động stack

```
cd hackathone-airquality-guardian
# .env phải có MONGODB_URI (password MỚI sau khi xoay 30/10) + DATADOG keys
docker compose up -d mock_openaq postgres airflow
docker compose ps          # airflow-init xong -> airflow "Up"
```

- Airflow UI: http://localhost:8080 (admin/admin)
- Mock OpenAQ: http://localhost:8000/v3 (mặc định `OPENAQ_API_URL` trong
  compose đã trỏ pipeline sang mock; muốn chạy data thật thì xóa dòng
  `OPENAQ_API_URL:` trong khối `airflow-env` của docker-compose.yml)
- Lần đầu: DAG "airquality_guardian" hiện ở UI; schedule */5.
  Muốn run ngay: nút "Trigger DAG" (không đợi schedule).

## 2. Kịch bản demo (5 bước, ~5 phút)

Mở 3 cửa sổ song song: **(A)** Airflow UI, **(B)** terminal demo,
**(C)** Datadog dashboard/monitor.

| Bước | (B) làm gì | (A) thấy gì | (C) thấy gì |
|---|---|---|---|
| 1. Bình thường | run 1 lần | run_cycle GREEN, 6 reading vào Atlas | `pipeline.records.inserted` tăng, status "running" |
| 2. Gây sự cố | `curl -X POST localhost:8000/control -d '{"fault":"500"}'` rồi trigger DAG | run_cycle **vẫn GREEN** (log: 3 retries) — vì engine đã self-heal | `selfhealing.quarantine.count` = 1; monitor "quarantine overflow" sắp chạm ngưỡng |
| 3. Giải thích | mở `quarantine_events` trên Atlas ( hoặc Atlas UI) | 1 record `status=pending` — **data không mất** | `pipeline_status.current.status=degraded` |
| 4. Hồi phục | `curl -X POST localhost:8000/control -d '{"fault":"ok"}'` trigger DAG | task2 `replay_quarantine` GREEN, log "Replay finished: processed=1 succeeded=1" | record -> `replayed`, `selfhealing.quarantine.count` = 0, monitor auto-resolve |
| 5. Chốt | `python -m src.main --report` (hoặc `docker compose run --rm pipeline python -m src.main --report`) | bảng AQI current + history |  |

Biến thể fault: `{"fault":"401"}` (lỗi permanent, không retry — 1 attempt
là degrade) và `{"fault":"timeout","delay":30}` (timeout).

## 3. Lưới an toàn (không cần Docker)

Nếu Docker/Atlas trục trặc vào giờ G:

```
python scripts/demo_selfhealing.py
```

Chạy story 5 bước trên HTTP thật (mock OpenAQ in-process) + DB fake,
in 19 checks qua/đối số. Chạy trên máy bất kỳ có venv, exit 0 = pass.

## 4. Dọn

```
docker compose down          # giữ volume pg
docker compose down -v       # xóa luôn metadata Airflow
```

## Ghi nhớ về thiết kế (đừng "sửa" các điều này)

- **Không set `retries` ở Airflow**: engine tự backoff+3 retries trong
  chu kỳ. Airflow chỉ "gắp chu kỳ kế tiếp".
- **Chu kỳ degraded = task GREEN**: dữ liệu đã được cứu vào
  `quarantine_events` (mất data = failure, mất chu kỳ = không).
  Cảnh báo nằm ở monitor Datadog, không phải màu task.
- **replay chạy mỗi chu kỳ**: idempotent (CAS claim/lease + unique
  dedup_key), no-op khi không có pending.
