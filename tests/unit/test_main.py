import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest

from src import main as pipeline_main


@pytest.mark.asyncio
async def test_main_prints_run_summary_and_sends_datadog_telemetry(
    monkeypatch,
):
    monkeypatch.setenv("DATADOG_API_KEY", "test-api-key")
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7
    fetcher = MagicMock()
    fetcher.fetch = AsyncMock(
        return_value={
            "results": [
                {
                    "value": 12.5,
                    "parameter_name": "pm25",
                    "unit": "µg/m³",
                    "datetime": {"utc": "2026-10-19T10:00:00Z"},
                }
            ]
        }
    )
    datadog = MagicMock()
    datadog.send_metrics.return_value = {"errors": []}

    monkeypatch.setattr(pipeline_main, "MongoDBClient", lambda: database)
    monkeypatch.setattr(pipeline_main, "DataFetcher", lambda: fetcher)
    monkeypatch.setattr(pipeline_main, "DatadogIntegrator", lambda: datadog)

    await pipeline_main.main([])

    database.insert_air_quality.assert_called_once()
    document = database.insert_air_quality.call_args.args[0]
    assert document["metadata"]["location_id"] == 4946813
    assert document["aqi"]["pm25_aqi"] == 52
    metrics = datadog.send_metrics.call_args.args[0]
    assert metrics["pipeline.records.fetched"] == 1
    assert metrics["pipeline.records.valid"] == 1
    assert metrics["pipeline.records.inserted"] == 1
    assert metrics["pipeline.records.failed"] == 0
    assert metrics["datavalue.aqi.current"] == 52
    log = datadog.send_logs.call_args.args[0]
    assert log["message"] == "AirQuality pipeline run completed"
    assert log["records_inserted"] == 1
    datadog.close.assert_called_once_with()
    database.close.assert_called_once_with()


def test_prune_expired_quarantine_calls_cleanup_and_survives_failures():
    pipeline = MagicMock()
    cleanup = pipeline.self_healing_engine.quarantine_manager.cleanup
    cleanup.return_value = 3

    pipeline_main.prune_expired_quarantine(pipeline)
    cleanup.assert_called_once()

    # A failing cleanup must never take the pipeline down.
    cleanup.side_effect = RuntimeError("mongo down")
    pipeline_main.prune_expired_quarantine(pipeline)  # must not raise
    assert cleanup.call_count == 2


@pytest.mark.asyncio
async def test_cycle_falls_back_to_stored_aqi_when_fully_deduped():
    """Bug-2 regression: a fully-deduped cycle has no fresh documents, so
    the 'datavalue.aqi.current' gauge used to vanish on exactly the
    cycles that repeat data. It must fall back to the newest stored doc."""
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7
    database.query_air_quality.return_value = [
        {
            "metadata": {"location_id": 4946813},
            "aqi": {"pm25_aqi": 88},
        }
    ]
    datadog = MagicMock()
    datadog.send_metrics.return_value = {"errors": []}

    pipeline = MagicMock()

    async def run_once(_params):
        return pipeline_main.PipelineRunResult(
            location_id=4946813,
            records_fetched=2,
            records_valid=2,
            records_inserted=0,
            records_failed=0,
            duration_seconds=0.1,
            documents=(),
            status="running",
            records_skipped=2,
        )

    pipeline.run_once = run_once
    pipeline.self_healing_engine.incidents = []

    await pipeline_main.run_cycle(database, datadog, pipeline, 4946813)

    sent_metrics = datadog.send_metrics.call_args.args[0]
    assert sent_metrics["datavalue.aqi.current"] == 88
    database.query_air_quality.assert_called_once_with(
        {"metadata.location_id": 4946813}, limit=1
    )


def test_run_cycle_prunes_expired_quarantine_before_fetching(monkeypatch):
    prune_calls = []
    monkeypatch.setattr(
        pipeline_main,
        "prune_expired_quarantine",
        lambda p: prune_calls.append(p),
    )
    database = MagicMock()
    datadog = MagicMock()
    pipeline = MagicMock()

    async def run_once(params):
        pipeline_run = pipeline_main.PipelineRunResult(
            location_id=4946813,
            records_fetched=0,
            records_valid=0,
            records_inserted=0,
            records_failed=0,
            duration_seconds=0.0,
            documents=(),
        )
        return pipeline_run

    pipeline.run_once = run_once

    asyncio.run(pipeline_main.run_cycle(database, datadog, pipeline, 4946813))
    assert prune_calls == [pipeline]


def test_build_pipeline_refuses_to_run_without_quarantine_collection():
    """2.8: the production wiring must never fall back to in-memory
    quarantine, where records would be lost on restart."""
    database = MagicMock()
    database.quarantine_events = None

    with pytest.raises(RuntimeError, match="not wired to MongoDB"):
        pipeline_main.build_pipeline(database)


def test_build_pipeline_wires_quarantine_to_mongo_collection():
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7

    _, engine = pipeline_main.build_pipeline(database)

    assert engine.quarantine_manager.collection is database.quarantine_events


def test_build_pipeline_shares_one_retention_setting_with_database():
    """A and B must use the same retention: the manager's TTL now comes
    from the database's .env-driven value, not the 30-day default."""
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7

    _, engine = pipeline_main.build_pipeline(database)

    assert engine.quarantine_manager.retention_days == 7
