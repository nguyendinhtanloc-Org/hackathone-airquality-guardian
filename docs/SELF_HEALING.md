# Kiến trúc Tự phục hồi Sự cố (Self-Healing Architecture)

## 1. Giới thiệu
Các pipeline dữ liệu IoT/Môi trường thường đối mặt với 3 bài toán lớn:
1. **API nguồn không ổn định**: Timeout, lỗi 5xx, mất kết nối mạng.
2. **Dữ liệu dị thường (Data anomalies)**: Cảm biến trả về số âm, NaN, inf hoặc vượt quá ngưỡng vật lý cho phép.
3. **Mất mát và trùng lặp dữ liệu**: Khi retry hoặc replay thủ công, dữ liệu dễ bị ghi đè hoặc nhân đôi.

**AirQuality Guardian** triển khai cơ chế Self-Healing 3 tầng (3-tier) bảo vệ toàn vẹn dữ liệu.

---

## 2. Mô hình 3 Tầng Tự Phục Hồi

```
[OpenAQ API] ──Fetch──> [Validation] ──Persist──> [MongoDB Atlas]
      │                       │                         │
  (Lỗi mạng/5xx)          (Dữ liệu rác)             (Dedup Gate)
      ▼                       ▼                         ▼
 [Tier 1: Retry]         [Bỏ qua rác,             [SHA-256 Check:
  Exponential Backoff     chỉ nạp phần sạch]       Chặn trùng lặp]
  (10s -> 20s -> 40s)         │
      │                       ▼
  (Thất bại cạn retry)   [Quarantine]
      ▼
 [Tier 2: Quarantine]
  Lưu vào MongoDB `quarantine_events`
  Đánh dấu pipeline: `degraded`
      │
      ▼
 [Tier 3: Replay Engine]
  CLI / Tự động chu kỳ sau
  CAS concurrency lock (pending -> in_flight -> replayed)
```

### Tầng 1: Retry Loop với Exponential Backoff (Khắc phục sự cố tạm thời)
- Khi gặp lỗi mạng, OpenAQ 5xx, hoặc Timeout:
  - Hệ thống tự động retry tối đa `MAX_RETRIES = 3` lần.
  - Khoảng chờ tăng theo hàm mũ: `10s` -> `20s` -> `40s`.
  - Tổng thời gian chờ tối đa 70s, nằm gọn trong chu kỳ lặp 300s của pipeline để không gây tắc nghẽn scheduler.
  - Các lỗi vĩnh viễn (4xx client error như 401 Unauthorized, 404 Not Found) sẽ không retry mà chuyển thẳng sang tầng 2.

### Tầng 2: Vùng cách ly sự cố (Quarantine Manager)
- Khi cạn số lần retry hoặc gặp lỗi không thể retry:
  - Pipeline **không bị sập (no crash)** mà chuyển trạng thái sang `degraded`.
  - Toàn bộ tham số và ngữ cảnh lỗi được đóng gói thành một `quarantine_event` lưu an toàn vào MongoDB.
  - Phát metric cảnh báo và ghi log `status=error` lên Datadog, kích hoạt Monitor cảnh báo.

### Tầng 3: Replay Engine & Idempotency (Phục hồi dữ liệu)
- Dữ liệu trong vùng cách ly có thể được replay tự động trong chu kỳ tới hoặc qua CLI:
  ```bash
  python -m src.main --replay --replay-limit 100
  ```
- **Bảo vệ đồng thời bằng CAS (Compare-And-Swap)**: Trạng thái chuyển từ `pending` sang `in_flight`. Nếu có 2 worker chạy song song, chỉ 1 worker được cấp lease xử lý bản ghi.
- **Lease Timeout (600s)**: Nếu worker bị crash giữa quá trình replay, bản ghi sẽ tự động được thu hồi quyền xử lý để worker khác nhận lại.
- **Trần số lần replay (`MAX_REPLAY_ATTEMPTS = 5`)**: Sau 5 lần replay thất bại liên tiếp, bản ghi chuyển trạng thái `failed` (terminal) để tránh vòng lặp vô hạn.
- **Chống trùng lặp (Dedup Key)**: Khóa SHA-256 độc nhất ngăn chặn triệt để việc sinh trùng bản ghi khi replay.

---

## 3. Bộ Giới hạn Kiểm tra Ngữ nghĩa & Vật lý (Validation Guardrails)
Bộ lọc `DataValidator` áp dụng ngưỡng vật lý EPA cho từng chất:
- Bác bỏ giá trị: `< 0`, `None`, `NaN`, `+inf`, `-inf`.
- Ngưỡng tối đa cho phép (`MAX_PLAUSIBLE`):
  - PM2.5: 1000 µg/m³
  - PM10: 1500 µg/m³
  - O3: 1000 µg/m³
  - NO2: 2000 µg/m³
  - CO: 100000 µg/m³
  - SO2: 2000 µg/m³
- Nguyên tắc: Dữ liệu bị lỗi ở 1 thông số không làm mất toàn bộ lần đọc; phần dữ liệu sạch của các cảm biến khác vẫn được chuyển đổi sang AQI và ghi nhận vào MongoDB.

---

## 4. Công cụ Giả lập Kiểm thử Tự Phục Hồi (`scripts/simulate.py`)
Hệ thống tích hợp sẵn script giả lập 11 kịch bản lỗi thực tế mà không cần tắt mạng hay sửa code:
1. `happy_path`: Luồng chạy bình thường.
2. `retry_success`: Giả lập OpenAQ timeout 2 lần rồi thành công ở lần 3.
3. `server_outage`: OpenAQ sập 500 liên tục -> chuyển sang quarantine -> status `degraded`.
4. `permanent_error`: OpenAQ trả mã 400 Bad Request -> không retry, quarantine ngay.
5. `dirty_data`: Sensor trả giá trị âm, NaN, vượt ngưỡng -> validator lọc sạch.
6. `partial_db_failure`: Lỗi ghi dở dang vào DB -> dedup marker rollback.
7. `duplicate_observations`: Gửi lại cùng dữ liệu -> Unique dedup marker chặn trùng.
8. `database_outage`: MongoDB ngừng hoạt động -> pipeline degraded.
9. `pollution_spike`: Ô nhiễm đột biến (AQI ~ 300) -> kiểm tra cảnh báo AQI.
10. `replay_recovered`: Khôi phục thành công các bản ghi từ quarantine.
11. `chaos`: Bật ngẫu nhiên tất cả các kịch bản lỗi liên tục.
