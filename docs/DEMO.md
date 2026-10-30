# Kịch bản demo cho giám khảo (~7 phút)

## A. Chuẩn bị (làm trước giờ demo 15–30 phút)

```powershell
cd E:\PJ\hackathone-airquality-guardian
.\venv\Scripts\activate
$env:PYTHONIOENCODING = "utf-8"
$env:DD_ENV = "demo"          # tách dữ liệu demo khỏi env:dev thật
python -m pytest -q           # phải thấy: 164 passed
python scripts\apply_dashboard.py
```

**Bơm dữ liệu nền** để dashboard không bị trống (metrics mất ~1–2 phút mới hiện):

```powershell
python scripts\simulate.py --scenario 1,1,5,9,1
```

Mở sẵn 3 cửa sổ:
1. **Terminal** (font to, nền tối) — đã activate venv + set 2 biến môi trường ở trên.
2. **Datadog dashboard** "AirQuality Guardian - Pipeline Health"
   - Đặt `$env = demo`, `$scenario = *`, khung thời gian **Past 15 minutes**, bật auto-refresh.
3. **MongoDB Atlas** (tuỳ chọn) — collection `air_quality`.

> [!IMPORTANT]
> Mỗi lần mở terminal mới phải set lại `$env:DD_ENV="demo"` và `$env:PYTHONIOENCODING="utf-8"`.

## B. Lời dẫn + thao tác

| Bước | Nói | Làm |
|---|---|---|
| 1. Bài toán (30s) | "Dữ liệu không khí từ sensor hay lỗi: API sập, timeout, giá trị rác. Pipeline thường sẽ chết hoặc mất dữ liệu." | Mở dashboard, chỉ ô **Current AQI** + **Pipeline status** |
| 2. Dữ liệu thật (1 phút) | "Đây là dữ liệu thật từ OpenAQ, trạm Hà Nội." | `python -m src.main` (chạy **1 lần**), rồi `python -m src.main --report` |
| 3. Mở simulator | "Để chứng minh khả năng tự phục hồi, chúng tôi giả lập sự cố ngay tại đây — dùng đúng code production." | `python scripts\simulate.py` → hiện menu |
| 4. Retry | "OpenAQ timeout 2 lần — hệ thống tự retry với exponential backoff, không mất dữ liệu." | Chọn **2** → chỉ `attempts=3`, `OK` |
| 5. Sập hẳn → Quarantine | "Nếu API sập hoàn toàn, hệ thống không crash mà cách ly yêu cầu lại." | Chọn **3** → `DEGRADED`, `quarantine pending: 1` |
| 6. Tự hồi phục | "Khi API sống lại, dữ liệu bị cách ly được replay tự động." | Chọn **10** → `replay summary: succeeded` |
| 7. Dữ liệu rác | "Sensor trả giá trị âm, NaN, vượt ngưỡng — bị loại, phần sạch vẫn lưu." | Chọn **5** → `valid=2 failed=4` |
| 8. Cảnh báo ô nhiễm | "Khi ô nhiễm đột biến, AQI lên mức nguy hại." | Chọn **9** → AQI ~300 |
| 9. Dashboard (1.5 phút) | "Mọi sự cố đều quan sát được." | Chuyển sang Datadog: nhóm **Self-healing** (retry, incident), **Logs** (incident stream), ô AQI chuyển đỏ. Đổi `$scenario` = `server_outage` để lọc |
| 10. Kết (30s) | "164 unit test, chạy được bằng Docker, dữ liệu dedup, không mất bản ghi." | Có thể chạy **11 (Chaos)** trong lúc trả lời câu hỏi |

> [!TIP]
> Metrics Datadog trễ 1–2 phút. Hãy chạy các bước 4–8 **trước**, nói tiếp phần giải thích, rồi mới chuyển sang dashboard ở bước 9.

## C. Phương án dự phòng

| Sự cố khi demo | Xử lý |
|---|---|
| Mất mạng / Wi-Fi hội trường yếu | `python scripts\simulate.py --no-datadog` — simulator vẫn chạy offline, chỉ show terminal; mở ảnh chụp dashboard đã chuẩn bị |
| OpenAQ thật bị chậm ở bước 2 | Bỏ qua, nói "đây chính là lý do cần self-healing" và chuyển thẳng sang simulator |
| Dashboard trống | Kiểm tra `$env = demo` và khung thời gian; chạy lại `--scenario 1,9` rồi đợi 1–2 phút |
| Chữ tiếng Việt lỗi font | Chưa set `$env:PYTHONIOENCODING="utf-8"` |
| Muốn xem log chi tiết | Thêm `--verbose` |

## D. Câu hỏi giám khảo hay gặp

- **"Dữ liệu giả có lẫn vào dữ liệu thật không?"** — Không. Simulator mặc định dùng DB in-memory, metrics gắn tag `source:simulator`, `scenario:*` và `env:demo`.
- **"Simulator có phải code riêng chỉ để demo?"** — Không. Chỉ OpenAQ được mock (qua `httpx.MockTransport`); fetcher, validator, AQI, self-healing, telemetry đều là code production.
- **"Retry vô hạn không?"** — Tối đa `MAX_RETRIES=3`, backoff mũ; lỗi 4xx không retry mà quarantine ngay; replay tối đa `MAX_REPLAY_ATTEMPTS=5` rồi đánh dấu `failed`.
- **"Replay có tạo bản ghi trùng?"** — Không, có dedup key unique index (kịch bản 7 chứng minh).
