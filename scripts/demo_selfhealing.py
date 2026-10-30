"""Self-healing demo: the full story, hermetically, on any machine.

Runs the REAL pipeline (fetcher + validator + pipeline engine +
quarantine manager + replay) against:
  - an in-process mock OpenAQ (scripts/mock_openaq/server.py) over a
    real HTTP socket, and
  - the fake in-memory Mongo collections the e2e suite uses.

No Atlas, no Docker, no Datadog needed — so it doubles as the safety
net on demo day when the infrastructure misbehaves. It exits 0 only
if every assertion holds.

Usage (from the repo root):
    python scripts/demo_selfhealing.py
"""

import asyncio
import json
import os
import sys
import threading
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from scripts.mock_openaq.server import make_server  # noqa: E402
from tests.e2e.test_pipeline_e2e import build_database  # noqa: E402
from src.fetcher.fetcher import DataFetcher  # noqa: E402
from src.main import replay_quarantined, run_cycle  # noqa: E402
from src.pipeline import Pipeline  # noqa: E402
from src.self_healing.engine import SelfHealingEngine  # noqa: E402
from src.self_healing.quarantine import QuarantineManager  # noqa: E402
from src.validation.validator import DataValidator  # noqa: E402

LOCATION_ID = 4946813
CHECKS = 0
FAILED = 0


def check(label: str, actual, expected) -> None:
    global CHECKS, FAILED
    CHECKS += 1
    ok = actual == expected
    FAILED += 0 if ok else 1
    mark = "  ok" if ok else "FAIL"
    print(f"  [{mark}] {label}: {actual!r}" + ("" if ok else f"  (expected {expected!r})"))


def banner(title: str) -> None:
    print(f"\n=== {title} " + "=" * max(0, 60 - len(title)))


def set_fault(control: str, payload: dict) -> None:
    request = urllib.request.Request(
        control, data=json.dumps(payload).encode(), method="POST"
    )
    urllib.request.urlopen(request, timeout=5).read()


async def run_story() -> int:
    global CHECKS, FAILED

    server = make_server(0)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    raw = f"http://127.0.0.1:{server.server_address[1]}"
    fetcher = DataFetcher(base_url=f"{raw}/v3", timeout=3)
    control = f"{raw}/control"

    database, _ = build_database()
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(collection=database.quarantine_events),
        max_retries=3,
    )
    pipeline = Pipeline(
        fetcher, DataValidator(), database,
        self_healing_engine=engine, backoff_seconds=0,
    )

    banner("STEP 1 - NORMAL OPERATION (mock station: 4 fresh + 2 stuck sensors)")
    r = await run_cycle(database, None, pipeline, LOCATION_ID)
    check("status", r.status, "running")
    check("inserted", r.records_inserted, 6)
    check("quarantine depth", database.count_pending_quarantine(), 0)

    banner("STEP 2 - SAME STATION, NEXT TICK (stuck sensors get deduped)")
    r = await run_cycle(database, None, pipeline, LOCATION_ID)
    check("status", r.status, "running")
    check("inserted (4 fresh)", r.records_inserted, 4)
    check("skipped_dedup (2 stuck)", r.records_skipped, 2)

    banner('STEP 3 - INCIDENT: OpenAQ is DOWN (POST /control {"fault": "500"})')
    set_fault(control, {"fault": "500"})
    r = await run_cycle(database, None, pipeline, LOCATION_ID)
    check("status (graceful)", r.status, "degraded")
    check("attempts (1 + 3 retries)", r.attempts, 4)
    check("inserted (data rescued, not lost)", r.records_inserted, 0)
    check("quarantine depth (+1 pending)", database.count_pending_quarantine(), 1)
    check("pipeline_status", database.pipeline_status.current.get("status"), "degraded")

    banner('STEP 4 - PERMANENT ERROR: bad credentials ({"fault": "401"})')
    set_fault(control, {"fault": "401"})
    r = await run_cycle(database, None, pipeline, LOCATION_ID)
    check("status", r.status, "degraded")
    check("attempts (no retry for 4xx)", r.attempts, 1)
    check("error type", r.incident.error_type, "AirQualityFetchError")
    check("quarantine depth (+1)", database.count_pending_quarantine(), 2)

    banner('STEP 5 - RECOVERY: OpenAQ back ({"fault": "ok"}) -> replay')
    set_fault(control, {"fault": "ok"})
    summary = await replay_quarantined(pipeline, engine, 100)
    check("replayed", summary["succeeded"], 2)
    check("failed", summary["failed"], 0)
    check("quarantine depth (drained)", database.count_pending_quarantine(), 0)
    check("recovery cycle status", (await run_cycle(database, None, pipeline, LOCATION_ID)).status, "running")

    server.shutdown()
    print(f"\n=== STORY COMPLETE: {CHECKS} checks, {FAILED} failed " + "=" * 20)
    if FAILED:
        print("DEMO FAILED - some assertions did not hold.")
        return 1
    print("All checks passed. The pipeline self-healed: no data lost,")
    print("no task crash, quarantine drained, dedup gate holds.")
    return 0


if __name__ == "__main__":
    # Demo-friendly logging: keep warnings (retries!) but drop INFO chatter.
    from loguru import logger
    logger.remove()
    logger.add(sys.stderr, level="WARNING")
    sys.exit(asyncio.run(run_story()))
