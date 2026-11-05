"""Integration tests: the REAL pipeline (fetcher -> validate -> persist)
talking HTTP to the mock OpenAQ service, pointed via OPENAQ_API_URL.

Unlike the e2e tests (which stop at an httpx MockTransport), these go
through a real socket against scripts/mock_openaq/server.py — which is
exactly the path docker-compose uses, where OPENAQ_API_URL points the
pipeline at the mock container. Mongo stays fake (the e2e fakes), so
these tests are hermetic and instant.
"""

import asyncio
import json
import threading
import urllib.request

import pytest

from tests.e2e.test_pipeline_e2e import build_database
from scripts.mock_openaq.server import make_server
from src.fetcher.fetcher import DataFetcher
from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.self_healing.quarantine import QuarantineManager
from src.validation.validator import DataValidator


LOCATION_ID = 4946813


class _MockEndpoints:
    """url = OpenAQ base the fetcher consumes; control = fault switch."""

    def __init__(self, raw_base: str):
        self.url = f"{raw_base}/v3"
        self.control = f"{raw_base}/control"


@pytest.fixture()
def mock(mock_base_url):
    return _MockEndpoints(mock_base_url)


@pytest.fixture()
def mock_base_url():
    server = make_server(0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()


def _set_fault(control_url: str, payload: dict) -> dict:
    request = urllib.request.Request(
        control_url,
        data=json.dumps(payload).encode(),
        method="POST",
    )
    return json.loads(urllib.request.urlopen(request, timeout=5).read())


def _pipeline(fetcher: DataFetcher, database, backoff=0.0) -> Pipeline:
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=getattr(database, "quarantine_events"),
        ),
        max_retries=3,
    )
    return Pipeline(
        fetcher,
        DataValidator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=backoff,
    )


@pytest.mark.asyncio
async def test_happy_cycle_inserts_fresh_and_dedups_stuck(mock):
    from src.main import run_cycle

    database, _ = build_database()
    fetcher = DataFetcher(base_url=mock.url)
    pipeline = _pipeline(fetcher, database)

    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    # 6 measurements: 4 fresh (unique this cycle) + 2 stuck sensors,
    # all new to this empty fake DB -> all inserted.
    assert result.status == "running"
    assert result.records_fetched == 6
    assert result.records_inserted == 6
    assert database.quarantine_events.documents == []

    # Next cycle: fresh sensors advanced (4 new docs), stuck sensors
    # returned the identical pinned reading -> dedup gate skips them,
    # mirroring the production skipped_dedup=2 on the stale sensors.
    result2 = await run_cycle(database, None, pipeline, LOCATION_ID)
    assert result2.status == "running"
    assert result2.records_inserted == 4
    assert result2.records_skipped == 2
    assert len(database.air_quality.documents) == 10


@pytest.mark.asyncio
async def test_500_fault_degrades_and_quarantines_once(mock):
    from src.main import run_cycle

    database, _ = build_database()
    fetcher = DataFetcher(base_url=mock.url, timeout=3)
    pipeline = _pipeline(fetcher, database)

    _set_fault(mock.control, {"fault": "500"})
    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    # Transient -> retried 3 times (backoff 0), then quarantined once
    # and reported degraded without crashing the scheduler.
    assert result.status == "degraded"
    assert result.attempts == 4
    assert result.records_inserted == 0
    assert result.incident is not None
    pending = [
        d for d in database.quarantine_events.documents if d["status"] == "pending"
    ]
    assert len(pending) == 1
    assert database.pipeline_status.current["status"] == "degraded"
    assert database.count_pending_quarantine() == 1


@pytest.mark.asyncio
async def test_401_fault_is_permanent_no_retries(mock):
    from src.main import run_cycle

    database, _ = build_database()
    fetcher = DataFetcher(base_url=mock.url, timeout=3)
    pipeline = _pipeline(fetcher, database)

    _set_fault(mock.control, {"fault": "401"})
    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    # Client errors are AirQualityFetchError (permanent): no retry,
    # degrade on the very first attempt.
    assert result.status == "degraded"
    assert result.attempts == 1
    assert result.incident.error_type == "AirQualityFetchError"
    assert database.count_pending_quarantine() == 1


@pytest.mark.asyncio
async def test_timeout_fault_degrades_via_retries(mock):
    from src.main import run_cycle

    database, _ = build_database()
    fetcher = DataFetcher(base_url=mock.url, timeout=1)
    pipeline = _pipeline(fetcher, database)

    # Mock holds the connection 3s; the fetcher's 1s timeout fires first.
    _set_fault(mock.control, {"fault": "timeout", "delay": 3})
    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    assert result.status == "degraded"
    assert result.attempts == 4
    assert result.incident.error_type == "TimeoutError"
    assert database.count_pending_quarantine() == 1


@pytest.mark.asyncio
async def test_recovery_replays_quarantined_record(mock):
    from src.main import replay_quarantined, run_cycle

    database, _ = build_database()
    fetcher = DataFetcher(base_url=mock.url, timeout=3)
    pipeline = _pipeline(fetcher, database)

    # Break, then recover.
    _set_fault(mock.control, {"fault": "500"})
    await run_cycle(database, None, pipeline, LOCATION_ID)
    assert database.count_pending_quarantine() == 1

    _set_fault(mock.control, {"fault": "ok"})
    summary = await replay_quarantined(pipeline, pipeline.self_healing_engine, 100)

    assert summary["succeeded"] == 1
    assert summary["failed"] == 0
    record = database.quarantine_events.documents[0]
    assert record["status"] == "replayed"
    assert database.count_pending_quarantine() == 0
    # The replayed fetch inserted real data this time.
    assert len(database.air_quality.documents) >= 1
