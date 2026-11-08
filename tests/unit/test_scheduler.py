"""Unit tests for the scheduler loop (stage 2.3)."""

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import main as pipeline_main


class FakeStopEvent:
    """asyncio.Event double whose wait() hangs, so only stop/interval end it."""

    def __init__(self, start_set: bool = False) -> None:
        self._set = start_set

    def is_set(self) -> bool:
        return self._set

    def set(self) -> None:
        self._set = True

    async def wait(self) -> None:
        while True:
            await asyncio.sleep(0)


@pytest.fixture(autouse=True)
def no_sleep():
    """Retry backoff and the cycle interval must never wait in tests."""
    with patch("src.pipeline.asyncio.sleep", new=_instant_sleep()):
        yield


def _instant_sleep():
    async def sleep(_seconds):
        return None

    return sleep


def make_pipeline(insert_count=1):
    pipeline = MagicMock()
    result = MagicMock()
    result.status = "running" if insert_count else "degraded"
    result.location_id = 4946813
    result.records_fetched = 3
    result.records_valid = 3
    result.records_inserted = insert_count
    result.records_failed = 0
    result.duration_seconds = 0.1
    result.documents = (
        {
            "metadata": {"location_id": 4946813},
            "timestamp": "2026-10-19T10:00:00Z",
            "measurements": {"pm25": 12.5},
            "aqi": {"pm25_aqi": 52},
        }
    )
    result.incident = None
    pipeline.run_once = AsyncMock(return_value=result)
    pipeline.self_healing_engine.incidents = []
    return pipeline


@pytest.mark.asyncio
async def test_run_loop_runs_max_cycles_then_stops():
    database = MagicMock()
    pipeline = make_pipeline()

    cycles = 0

    async def fake_cycle(db, datadog, pl, location_id):
        nonlocal cycles
        cycles += 1
        return None

    with patch.object(pipeline_main, "run_cycle", new=fake_cycle):
        await pipeline_main.run_loop(
            database,
            None,
            pipeline,
            4946813,
            interval_seconds=0,
            stop_event=FakeStopEvent(),
            max_cycles=2,
        )

    assert cycles == 2
    database.update_pipeline_status.assert_called_once_with("stopped")


@pytest.mark.asyncio
async def test_run_loop_stops_when_stop_event_set_before_start():
    database = MagicMock()
    pipeline = make_pipeline()

    with patch.object(pipeline_main, "run_cycle", new=AsyncMock()):
        await pipeline_main.run_loop(
            database,
            None,
            pipeline,
            4946813,
            stop_event=FakeStopEvent(start_set=True),
        )

    # No cycle runs, but the stopped status is still recorded.
    pipeline.run_once.assert_not_awaited()
    database.update_pipeline_status.assert_called_once_with("stopped")


@pytest.mark.asyncio
async def test_failing_cycle_does_not_kill_scheduler():
    database = MagicMock()
    pipeline = MagicMock()

    async def boom(*_args, **_kwargs):
        raise RuntimeError("everything on fire")

    with patch.object(pipeline_main, "run_cycle", new=boom):
        await pipeline_main.run_loop(
            database,
            None,
            pipeline,
            4946813,
            interval_seconds=0,
            stop_event=FakeStopEvent(),
            max_cycles=3,
        )

    # The scheduler survived three failing cycles and still stopped cleanly.
    database.update_pipeline_status.assert_called_once_with("stopped")


@pytest.mark.asyncio
async def test_stop_during_sleep_returns_immediately():
    """Wakeup from the inter-cycle sleep on stop_event set."""
    database = MagicMock()
    pipeline = make_pipeline()
    stop_event = asyncio.Event()

    real_wait_for = asyncio.wait_for
    cycles = {"n": 0}

    async def fake_cycle(*_args, **_kwargs):
        cycles["n"] += 1
        return None

    async def wait_for_that_stops_early(*args, **kwargs):
        stop_event.set()
        return await real_wait_for(*args, **kwargs)

    with patch.object(pipeline_main, "run_cycle", new=fake_cycle), \
         patch.object(asyncio, "wait_for", new=wait_for_that_stops_early):
        started = asyncio.get_event_loop().time()
        await pipeline_main.run_loop(
            database,
            None,
            pipeline,
            4946813,
            interval_seconds=999,  # would take ~16 min without the stop
            stop_event=stop_event,
        )
        elapsed = asyncio.get_event_loop().time() - started

    assert cycles["n"] == 1
    assert elapsed < 1.0  # stopped instantly, did not wait 999s
    database.update_pipeline_status.assert_called_once_with("stopped")


@pytest.mark.asyncio
async def test_main_with_loop_flag_runs_scheduler(monkeypatch):
    database = MagicMock()
    pipeline = make_pipeline()
    engine = MagicMock()
    engine.incidents = []

    monkeypatch.setattr(pipeline_main, "MongoDBClient", lambda: database)
    monkeypatch.delenv("DATADOG_API_KEY", raising=False)
    monkeypatch.setattr(
        pipeline_main,
        "build_pipeline",
        lambda _db: (pipeline, engine),
    )
    with patch.object(pipeline_main, "run_loop", new=AsyncMock()) as mock_loop:
        await pipeline_main.main(["--loop"])

    mock_loop.assert_awaited_once()
    loop_args = mock_loop.call_args.args
    assert loop_args[0] is database
    assert loop_args[1] is None  # no Datadog without an API key
    assert loop_args[2] is pipeline
    assert loop_args[3] == 4946813
    database.close.assert_called_once_with()
