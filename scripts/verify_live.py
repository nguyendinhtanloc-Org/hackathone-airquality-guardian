"""Live infrastructure verification for AirQuality Guardian.

Chạy trên máy CÓ MẠNG (sandbox/CI không chạy được):
    python scripts/verify_live.py

Mục đích: bắt các bug mà test mock KHÔNG thấy được (schema lệch Atlas thật,
quyền thiếu, endpoint OpenAQ đổi, index collide, Datadog API thay đổi).

Kết quả: in từng check [OK]/[WARN]/[FAIL]; exit 0 nếu không có [FAIL].
"""

from __future__ import annotations

import asyncio
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

PASS, FAIL, WARN = "OK", "FAIL", "WARN"
results: list[tuple[str, str, str]] = []


def report(kind: str, name: str, detail: str) -> None:
    results.append((kind, name, detail))
    print(f"[{kind}] {name}: {detail}")


def main() -> int:
    load_dotenv(ROOT / ".env")

    # ------------------------------------------------------------------
    # 1. MongoDB Atlas: kết nối + index
    # ------------------------------------------------------------------
    database = None
    try:
        from src.database.mongodb import MongoDBClient

        database = MongoDBClient()
        ping = database.client.admin.command("ping")
        report(
            PASS,
            "atlas:connect",
            f"ping ok={ping.get('ok')} db={database.database.name}",
        )
    except Exception as exc:  # noqa: BLE001
        report(FAIL, "atlas:connect", f"{type(exc).__name__}: {exc}")
        return finish()

    # ------------------------------------------------------------------
    # 2. DEDUP THẬT: cùng 1 observation insert 2 lần -> lần 2 phải None
    #    (điểm then chốt của fix marker-first: mock chỉ chứng minh wiring)
    # ------------------------------------------------------------------
    document: dict[str, Any] = {
        "event_id": "verify-live-0001",
        "metadata": {"location_id": 4946813},
        "timestamp": datetime(2026, 10, 29, 0, 0, 0, tzinfo=timezone.utc),
        "measurements": {"pm25": 42.0, "pm25_unit": "µg/m³"},
        "aqi": {"pm25_aqi": 84.0, "primary_pollutant": "pm25"},
    }
    try:
        key = database._dedup_key(document)
        first_id = database.insert_air_quality(document)
        second_id = database.insert_air_quality(document)
        if first_id is not None and second_id is None:
            report(PASS, "atlas:dedup",
                   "insert #1 ok, insert #2 (same dedup_key) -> None")
        else:
            report(FAIL, "atlas:dedup",
                   f"first={first_id} second={second_id} "
                   "=> record TRÙNG bị lưu lại, dedup không hoạt động")
        # dọn test data (quan sát + marker) để khỏi bẩn DB
        database.air_quality.delete_one({"event_id": document["event_id"]})
        database.air_quality_dedup.delete_one({"dedup_key": key})
    except Exception as exc:  # noqa: BLE001
        report(FAIL, "atlas:dedup", f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # 3. Pipeline: 1 cycle THẬT (OpenAQ -> Atlas -> pipeline_status)
    # ------------------------------------------------------------------
    try:
        from src.main import build_pipeline, run_cycle
        from src.datadog.integrator import DatadogIntegrator

        pipeline, _ = build_pipeline(database)
        datadog = DatadogIntegrator() if os.getenv("DATADOG_API_KEY") else None

        result = asyncio.run(run_cycle(database, datadog, pipeline, 4946813))
        detail = (
            f"fetched={result.records_fetched} valid={result.records_valid} "
            f"inserted={result.records_inserted} status={result.status} "
            f"attempts={result.attempts}"
        )
        if result.status == "running" and result.records_inserted > 0:
            report(PASS, "pipeline:cycle", detail)
        elif result.incident is not None:
            report(WARN, "pipeline:cycle",
                   f"{detail} incident={result.incident.error_type}: "
                   f"{str(result.incident.error)[:120]}")
        else:
            report(WARN, "pipeline:cycle", detail)

        status = database.pipeline_status.find_one({"_id": "current"}) or {}
        report(PASS, "atlas:status",
               f"pipeline_status.current.status = {status.get('status')!r}")
    except Exception as exc:  # noqa: BLE001
        report(FAIL, "pipeline:cycle", f"{type(exc).__name__}: {exc}")

    # ------------------------------------------------------------------
    # 4. Datadog (nếu có key): metrics thật
    # ------------------------------------------------------------------
    if os.getenv("DATADOG_API_KEY"):
        try:
            from src.datadog.integrator import DatadogIntegrator as DD

            dd = DD()
            dd.send_metrics(
                {"verify.live.ping": 1},
                tags=["env:dev", "service:airquality-pipeline"],
            )
            report(PASS, "datadog:metrics", "send_metrics accepted")
            dd.close()
        except Exception as exc:  # noqa: BLE001
            report(FAIL, "datadog:metrics", f"{type(exc).__name__}: {exc}")
    else:
        report(WARN, "datadog", "không có DATADOG_API_KEY trong .env - bỏ qua")

    blocking = [r for r in results if r[0] == FAIL]
    print()
    print(f"=== TỔNG KẾT: "
          f"{sum(1 for r in results if r[0] == PASS)} OK, "
          f"{sum(1 for r in results if r[0] == WARN)} WARN, "
          f"{len(blocking)} FAIL ===")
    for _, name, detail in blocking:
        print(f"  - {name}: {detail}")
    return finish()


def finish() -> int:
    return 0 if all(r[0] != FAIL for r in results) else 1


if __name__ == "__main__":
    sys.exit(main())
