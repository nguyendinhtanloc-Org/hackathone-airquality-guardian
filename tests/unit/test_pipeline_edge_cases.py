"""Regression tests for edge cases found during code review.

Covers:
- MongoDB insert failure mid-run (partial writes must not crash the caller)
- fetched>0 but inserted==0 must surface as degraded (silent data loss)
- empty response is a healthy run, not degraded
- engine.incidents is reset between runs (scheduler must not leak memory)
- incident_id is assigned so incidents are traceable
- validator rejects negative / implausible / non-finite values
"""

from unittest.mock import MagicMock, patch

import pytest

from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.validation.validator import DataValidator


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("src.pipeline.asyncio.sleep", new=_instant_sleep()):
        yield


def _instant_sleep():
    async def sleep(_seconds):
        return None

    return sleep


@pytest.fixture
def engine():
    return SelfHealingEngine(max_retries=0)


@pytest.fixture
def database():
    return MagicMock()


def build_pipeline(engine, database, fetch, **kwargs):
    fetcher = MagicMock()
    fetcher.fetch = fetch
    return Pipeline(
        fetcher,
        DataValidator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
        **kwargs,
    )


def _results(*rows):
    return {"results": list(rows)}


def _row(value=12.5, parameter="pm25", utc="2026-10-19T10:00:00Z"):
    return {
        "value": value,
        "parameter_name": parameter,
        "unit": "µg/m³",
        "datetime": {"utc": utc},
    }


@pytest.mark.asyncio
async def test_database_insert_failure_returns_degraded(
    engine, database
):
    calls = {"n": 0}

    def insert(document):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("Mongo connection lost")

    database.insert_air_quality.side_effect = insert
    pipeline = build_pipeline(
        engine,
        database,
        _returning(_results(_row(), _row(), _row())),
    )

    result = await pipeline.run_once({"location_id": 4946813})

    # The crash is absorbed, not raised.
    assert result.status == "degraded"
    assert result.incident is not None
    assert result.incident.component == "database"
    assert calls["n"] == 2
    # A partial write really did store 1 record before the crash: report it
    # rather than pretending nothing happened (dedup makes re-emitting safe).
    assert result.records_inserted == 1
    assert result.records_failed == 2
    assert len(result.documents) == 1


def _returning(payload):
    async def fetch(_params):
        return payload

    return fetch


@pytest.mark.asyncio
async def test_partial_write_is_reported_in_log_context(
    engine, database
):
    pipeline = build_pipeline(
        engine,
        database,
        _returning(_results(_row(utc=""), _row())),
    )

    result = await pipeline.run_once()

    # First record has a bad timestamp -> crash after 0 inserts.
    assert result.status == "degraded"
    assert result.records_fetched == 2
    database.insert_air_quality.assert_not_called()


@pytest.mark.asyncio
async def test_fetched_but_nothing_stored_is_degraded(engine, database):
    """Data arrived but was all rejected: the run is degraded, not healthy."""
    pipeline = build_pipeline(
        engine,
        database,
        _returning(_results(_row(value=-5), _row(value=9999))),
    )

    result = await pipeline.run_once()

    assert result.records_fetched == 2
    assert result.records_valid == 0
    assert result.records_inserted == 0
    assert result.records_failed == 2
    assert result.status == "degraded"


@pytest.mark.asyncio
async def test_empty_response_is_a_healthy_run(engine, database):
    pipeline = build_pipeline(engine, database, _returning(_results()))

    result = await pipeline.run_once()

    assert result.status == "running"
    assert result.records_fetched == 0
    assert result.incident is None


@pytest.mark.asyncio
async def test_incidents_are_reset_between_runs(engine, database):
    async def failing_fetch(_params):
        raise TimeoutError("timed out")

    pipeline = build_pipeline(engine, database, failing_fetch)

    for _ in range(5):
        await pipeline.run_once()

    # One incident per run, not a cumulative list (no memory leak, and
    # update_pipeline_status only carries the current run's incidents).
    assert len(engine.incidents) == 1
    assert engine.incidents[0].error_type == "TimeoutError"


@pytest.mark.asyncio
async def test_incident_id_is_assigned(engine, database):
    async def failing_fetch(_params):
        raise TimeoutError("timed out")

    pipeline = build_pipeline(engine, database, failing_fetch)

    await pipeline.run_once()

    incident = engine.incidents[0]
    assert incident.incident_id is not None
    assert len(incident.incident_id) == 36
    quarantined = next(
        iter(engine.quarantine_manager._memory_store.values())
    )
    assert quarantined["metadata"]["incident_id"] == incident.incident_id


def test_validator_rejects_negative_value():
    validator = DataValidator()

    result = validator.validate(_results(_row(value=-5)))

    assert result == []


def test_validator_rejects_implausible_value():
    validator = DataValidator()

    result = validator.validate(_results(_row(value=9999, parameter="pm25")))

    assert result == []


def test_validator_rejects_none_value():
    validator = DataValidator()

    result = validator.validate(_results(_row(value=None)))

    assert result == []


def test_validator_rejects_non_finite_value():
    validator = DataValidator()

    result = validator.validate(_results(_row(value=float("inf"))))

    assert result == []


def test_validator_accepts_plausible_value():
    validator = DataValidator()

    result = validator.validate(_results(_row(value=999.5, parameter="pm25")))

    assert len(result) == 1
    assert result[0].value == 999.5
