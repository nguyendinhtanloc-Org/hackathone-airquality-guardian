"""Unit tests for the ``--replay`` command line flow."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import main as pipeline_main
from src.self_healing.engine import SelfHealingEngine
from src.self_healing.quarantine import QuarantineManager


@pytest.fixture(autouse=True)
def no_sleep():
    """Keep retry backoff from actually waiting during the tests."""
    with patch("src.pipeline.asyncio.sleep", new=_instant_sleep()):
        yield


def _instant_sleep():
    async def sleep(_seconds):
        return None

    return sleep


def make_engine_with_pending(data, **kwargs):
    engine = SelfHealingEngine(max_retries=3, **kwargs)
    engine.quarantine_manager.add_to_quarantine(
        data=data,
        error=TimeoutError("previous failure"),
        metadata={"component": "fetcher"},
    )
    return engine


def make_pipeline(engine, fetch_result=None, insert_count=1):
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock(return_value=make_result(insert_count))
    pipeline.self_healing_engine = engine
    return pipeline


def make_result(insert_count):
    result = MagicMock()
    result.status = "running" if insert_count else "degraded"
    result.records_inserted = insert_count
    return result


# --- argument parsing -------------------------------------------------


def test_parse_args_defaults():
    args = pipeline_main.parse_args([])

    assert args.replay is False
    assert args.replay_limit == 100
    assert args.location_id == 4946813


def test_parse_args_replay_flag():
    args = pipeline_main.parse_args(["--replay", "--replay-limit", "5"])

    assert args.replay is True
    assert args.replay_limit == 5


def test_parse_args_location_id_override():
    args = pipeline_main.parse_args(["--location-id", "1234"])

    assert args.location_id == 1234


def test_parse_args_falls_back_to_process_arguments(monkeypatch):
    monkeypatch.setattr("sys.argv", ["src.main", "--replay", "--replay-limit", "7"])

    args = pipeline_main.parse_args()

    assert args.replay is True
    assert args.replay_limit == 7


# --- replay behaviour -------------------------------------------------


@pytest.mark.asyncio
async def test_replay_marks_record_replayed_after_success():
    engine = make_engine_with_pending({"location_id": 4946813})
    pipeline = make_pipeline(engine)

    summary = await pipeline_main.replay_quarantined(pipeline, engine, limit=100)

    assert summary == {"processed": 1, "succeeded": 1, "failed": 0}
    record = next(iter(engine.quarantine_manager._memory_store.values()))
    assert record["status"] == "replayed"
    assert "replayed_at" in record


@pytest.mark.asyncio
async def test_replay_keeps_record_pending_when_pipeline_fails():
    engine = make_engine_with_pending({"location_id": 4946813})
    pipeline = make_pipeline(engine, insert_count=0)

    summary = await pipeline_main.replay_quarantined(pipeline, engine, limit=100)

    assert summary == {"processed": 1, "succeeded": 0, "failed": 1}
    record = next(iter(engine.quarantine_manager._memory_store.values()))
    assert record["status"] == "pending"
    assert "did not persist data" in record["last_replay_error"]


@pytest.mark.asyncio
async def test_replay_passes_quarantined_data_to_pipeline():
    engine = make_engine_with_pending({"location_id": 42})
    pipeline = make_pipeline(engine)

    await pipeline_main.replay_quarantined(pipeline, engine, limit=100)

    pipeline.run_once.assert_awaited_once_with({"location_id": 42})


@pytest.mark.asyncio
async def test_replay_handles_non_dict_quarantined_data():
    engine = make_engine_with_pending("not-a-dict")
    pipeline = make_pipeline(engine)

    summary = await pipeline_main.replay_quarantined(pipeline, engine, limit=100)

    assert summary["succeeded"] == 1
    pipeline.run_once.assert_awaited_once_with({})


@pytest.mark.asyncio
async def test_replay_skips_records_that_are_not_pending():
    engine = make_engine_with_pending({"location_id": 4946813})
    record = next(iter(engine.quarantine_manager._memory_store.values()))
    engine.quarantine_manager._update_record(
        record["quarantine_id"], status="replayed"
    )

    summary = await pipeline_main.replay_quarantined(pipeline := make_pipeline(engine), engine, limit=100)

    assert summary == {"processed": 0, "succeeded": 0, "failed": 0}
    pipeline.run_once.assert_not_awaited()


@pytest.mark.asyncio
async def test_replay_increments_replay_attempts():
    engine = make_engine_with_pending({"location_id": 4946813})
    pipeline = make_pipeline(engine, insert_count=0)

    await pipeline_main.replay_quarantined(pipeline, engine, limit=100)

    record = next(iter(engine.quarantine_manager._memory_store.values()))
    assert record["replay_attempts"] == 1


# --- main() routing ---------------------------------------------------


@pytest.mark.asyncio
async def test_main_replay_flag_skips_normal_run(monkeypatch):
    database = MagicMock()
    datadog = MagicMock()
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock()
    engine = make_engine_with_pending({"location_id": 4946813})
    pipeline.self_healing_engine = engine

    monkeypatch.setattr(pipeline_main, "MongoDBClient", lambda: database)
    monkeypatch.setattr(pipeline_main, "DatadogIntegrator", lambda: datadog)
    monkeypatch.setattr(
        pipeline_main,
        "build_pipeline",
        lambda _database: (pipeline, engine),
    )

    await pipeline_main.main(["--replay"])

    # Replay chạy qua pipeline.run_once, nhưng không chạy vòng fetch chính.
    pipeline.run_once.assert_awaited_once_with({"location_id": 4946813})
    database.update_pipeline_status.assert_not_called()
    database.close.assert_called_once_with()


@pytest.mark.asyncio
async def test_main_without_replay_runs_pipeline(monkeypatch):
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7
    fetcher = MagicMock()
    fetcher.fetch = AsyncMock(return_value={"results": []})

    monkeypatch.setattr(pipeline_main, "MongoDBClient", lambda: database)
    monkeypatch.setattr(pipeline_main, "DataFetcher", lambda: fetcher)
    monkeypatch.delenv("DATADOG_API_KEY", raising=False)

    await pipeline_main.main([])

    database.insert_air_quality.assert_not_called()
    database.update_pipeline_status.assert_called_once()
    assert database.update_pipeline_status.call_args.args[0] == "running"
