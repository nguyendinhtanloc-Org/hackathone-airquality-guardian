"""Unit tests for the self-healing wiring inside the pipeline."""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.validation.validator import DataValidator


@pytest.fixture(autouse=True)
def no_sleep():
    """Keep retry backoff from actually waiting during the tests."""
    with patch("src.pipeline.asyncio.sleep", new=_instant_sleep()):
        yield


def _instant_sleep():
    async def sleep(_seconds):
        return None

    return sleep


@pytest.fixture
def database():
    return MagicMock()


@pytest.fixture
def engine():
    return SelfHealingEngine()


def build_pipeline(engine, database, fetch):
    fetcher = MagicMock()
    fetcher.fetch = fetch
    return Pipeline(
        fetcher,
        DataValidator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
    )


@pytest.mark.asyncio
async def test_fetch_failure_returns_incident(engine, database):
    fetcher = MagicMock()
    fetcher.fetch = _raising(ConnectionError("connection reset"))

    pipeline = build_pipeline(engine, database, fetcher.fetch)
    result = await pipeline.run_once({"location_id": 4946813})

    assert result.incident is not None
    assert result.status == "degraded"
    assert result.records_fetched == 0
    assert result.records_valid == 0
    assert result.records_inserted == 0
    assert result.records_failed == 0
    assert result.documents == ()
    assert result.location_id == 4946813
    database.insert_air_quality.assert_not_called()


@pytest.mark.asyncio
async def test_pipeline_does_not_raise_on_fetch_failure(engine, database):
    pipeline = build_pipeline(
        engine, database, _raising(TimeoutError("timed out"))
    )

    result = await pipeline.run_once()

    assert result.incident is not None
    assert result.incident.error_type == "TimeoutError"
    assert result.incident.component == "fetcher"


@pytest.mark.asyncio
async def test_transient_fetch_failure_is_quarantined_after_retries_exhausted(
    engine, database
):
    pipeline = build_pipeline(
        engine, database, _raising(TimeoutError("timed out"))
    )

    await pipeline.run_once()

    quarantined = list(engine.quarantine_manager._memory_store.values())
    assert len(quarantined) == 1
    assert quarantined[0]["status"] == "pending"
    assert engine.incidents[-1].retryable is True


@pytest.mark.asyncio
async def test_permanent_fetch_failure_is_quarantined(engine, database):
    pipeline = build_pipeline(
        engine, database, _raising(ValueError("bad response shape"))
    )

    await pipeline.run_once()

    quarantined = list(engine.quarantine_manager._memory_store.values())
    assert len(quarantined) == 1
    assert quarantined[0]["status"] == "pending"


@pytest.mark.asyncio
async def test_incident_is_recorded_in_engine(engine, database):
    pipeline = build_pipeline(
        engine, database, _raising(ValueError("bad response shape"))
    )

    await pipeline.run_once()

    assert len(engine.incidents) == 1
    assert engine.incidents[0].error_type == "ValueError"
    assert engine.incidents[0].retryable is False


@pytest.mark.asyncio
async def test_successful_run_has_no_incident(engine, database):
    payload = {
        "results": [
            {
                "value": 12.5,
                "parameter_name": "pm25",
                "unit": "µg/m³",
                "datetime": {"utc": "2026-10-19T10:00:00Z"},
            }
        ]
    }
    pipeline = build_pipeline(engine, database, _returning(payload))

    result = await pipeline.run_once({"location_id": 4946813})

    assert result.incident is None
    assert result.status == "running"
    assert result.records_inserted == 1
    assert engine.incidents == []
    assert engine.quarantine_manager._memory_store == {}


@pytest.mark.asyncio
async def test_pipeline_uses_default_engine_when_none_given(database):
    fetcher = MagicMock()
    fetcher.fetch = _raising(ValueError("bad response shape"))

    pipeline = Pipeline(
        fetcher, DataValidator(), database, backoff_seconds=0
    )

    result = await pipeline.run_once()

    assert result.incident is not None
    assert pipeline.self_healing_engine is not None


def _raising(error):
    async def fetch(_params):
        raise error

    return fetch


def _returning(payload):
    async def fetch(_params):
        return payload

    return fetch
