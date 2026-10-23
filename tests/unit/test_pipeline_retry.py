"""Unit tests for the retry loop and exponential backoff in the pipeline."""

from unittest.mock import MagicMock, patch

import pytest

from src.fetcher.fetcher import AirQualityFetchError
from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.validation.validator import DataValidator


@pytest.fixture
def database():
    return MagicMock()


@pytest.fixture
def engine():
    return SelfHealingEngine(max_retries=3)


@pytest.fixture
def delays():
    """Capture the sleep durations the pipeline asks for."""
    return []


@pytest.fixture(autouse=True)
def patched_sleep(delays):
    with patch("src.pipeline.asyncio.sleep", new=_record_sleep(delays)):
        yield


def _record_sleep(delays):
    async def sleep(seconds):
        delays.append(seconds)

    return sleep


def build_pipeline(engine, database, fetch, backoff_seconds=30):
    fetcher = MagicMock()
    fetcher.fetch = fetch
    return Pipeline(
        fetcher,
        DataValidator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=backoff_seconds,
    )


def failing_then_succeeding(failures, error):
    """Fail with ``error`` the first N calls, then return a payload."""
    state = {"calls": 0}

    async def fetch(_params):
        state["calls"] += 1
        if state["calls"] <= failures:
            raise error
        return {"results": []}

    fetch.state = state
    return fetch


def always_failing(error):
    state = {"calls": 0}

    async def fetch(_params):
        state["calls"] += 1
        raise error

    fetch.state = state
    return fetch


@pytest.mark.asyncio
async def test_retries_transient_failure_then_succeeds(engine, database, delays):
    fetch = failing_then_succeeding(2, TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch)

    result = await pipeline.run_once()

    assert fetch.state["calls"] == 3
    assert result.incident is None
    assert result.status == "running"
    assert result.attempts == 3
    assert engine.quarantine_manager._memory_store == {}


@pytest.mark.asyncio
async def test_gives_up_after_max_retries(engine, database, delays):
    fetch = always_failing(ConnectionError("connection reset"))
    pipeline = build_pipeline(engine, database, fetch)

    result = await pipeline.run_once()

    assert fetch.state["calls"] == 4  # 1 initial attempt + 3 retries
    assert result.incident is not None
    assert result.status == "degraded"
    assert result.attempts == 4
    assert result.incident.retryable is True
    assert len(engine.quarantine_manager._memory_store) == 1


@pytest.mark.asyncio
async def test_permanent_failure_is_not_retried(engine, database, delays):
    fetch = always_failing(AirQualityFetchError("404 Not Found"))
    pipeline = build_pipeline(engine, database, fetch)

    result = await pipeline.run_once()

    assert fetch.state["calls"] == 1
    assert result.incident is not None
    assert result.incident.retryable is False
    assert result.attempts == 1
    assert len(engine.quarantine_manager._memory_store) == 1
    assert delays == []


@pytest.mark.asyncio
async def test_backoff_delays_grow_exponentially(engine, database, delays):
    fetch = always_failing(TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch, backoff_seconds=30)

    await pipeline.run_once()

    assert delays == [30, 60, 120]


@pytest.mark.asyncio
async def test_backoff_respects_configured_base(engine, database, delays):
    fetch = always_failing(TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch, backoff_seconds=5)

    await pipeline.run_once()

    assert delays == [5, 10, 20]


@pytest.mark.asyncio
async def test_attempt_counter_increments(engine, database, delays):
    fetch = always_failing(TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch)

    await pipeline.run_once()

    assert [incident.attempt for incident in engine.incidents] == [0, 1, 2, 3]


@pytest.mark.asyncio
async def test_incident_records_component_and_error_type(engine, database, delays):
    fetch = always_failing(TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch)

    await pipeline.run_once()

    for incident in engine.incidents:
        assert incident.component == "fetcher"
        assert incident.error_type == "TimeoutError"
        assert incident.retryable is True


@pytest.mark.asyncio
async def test_successful_first_try_does_not_sleep(engine, database, delays):
    fetch = failing_then_succeeding(0, TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch)

    result = await pipeline.run_once()

    assert fetch.state["calls"] == 1
    assert result.attempts == 1
    assert delays == []


@pytest.mark.asyncio
async def test_zero_max_retries_disables_retrying(engine, database, delays):
    engine.max_retries = 0
    fetch = always_failing(TimeoutError("timed out"))
    pipeline = build_pipeline(engine, database, fetch)

    result = await pipeline.run_once()

    assert fetch.state["calls"] == 1
    assert result.attempts == 1
    assert delays == []
