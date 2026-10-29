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


# --- 2.13 --report -------------------------------------------------


def _stored_doc(location_id, measurements, aqi):
    return {
        "metadata": {"location_id": location_id},
        "timestamp": __import__("datetime").datetime(2026, 10, 29, 9, 20),
        "measurements": measurements,
        "aqi": aqi,
    }


def test_build_aqi_report_summarises_newest_doc_and_history():
    database = MagicMock()
    database.query_air_quality.return_value = [
        _stored_doc(
            4946813,
            {"pm25": 12.5, "pm25_unit": "µg/m³", "o3": 40.0, "o3_unit": "µg/m³"},
            {"pm25_aqi": 52, "o3_aqi": 32},
        )
    ]
    database.aggregate_aqi.return_value = [
        {"_id": 4946813, "average_aqi": 44.0, "min_aqi": 30, "max_aqi": 58, "records": 12}
    ]

    report = pipeline_main.build_aqi_report(database, 4946813, limit=10)

    assert report["location_id"] == 4946813
    # newest doc drives 'current'
    assert report["current"]["aqi"] == 52  # max of pm25(52) / o3(32)
    assert report["current"]["primary_pollutant"] == "pm25"
    # per-pollutant breakdown. Note the AQI is recomputed from the raw
    # measurement via calculate_all (EPA breakpoints), not read from the
    # stored aqi dict: o3=40.0 -> 19, pm25=12.5 -> 52.
    by_param = {p["parameter"]: p for p in report["per_pollutant"]}
    assert by_param["pm25"]["aqi"] == 52
    assert by_param["o3"]["aqi"] == 19
    # history from the aggregate
    assert report["history"][0]["average_aqi"] == 44.0
    # both DB helpers queried with the location filter
    database.query_air_quality.assert_called_once_with(
        {"metadata.location_id": 4946813}, limit=10
    )
    database.aggregate_aqi.assert_called_once_with(
        {"metadata.location_id": 4946813}
    )


def test_build_aqi_report_handles_no_stored_data():
    database = MagicMock()
    database.query_air_quality.return_value = []
    database.aggregate_aqi.return_value = []

    report = pipeline_main.build_aqi_report(database, 4946813, limit=10)

    assert report["current"] is None
    assert report["per_pollutant"] == []
    assert report["history"] == []


def test_format_aqi_report_renders_tables():
    database = MagicMock()
    database.query_air_quality.return_value = [
        _stored_doc(
            4946813,
            {"pm25": 12.5, "pm25_unit": "µg/m³"},
            {"pm25_aqi": 52},
        )
    ]
    database.aggregate_aqi.return_value = [
        {"_id": 4946813, "average_aqi": 44.0, "min_aqi": 30, "max_aqi": 58, "records": 12}
    ]

    text = pipeline_main.format_aqi_report(
        pipeline_main.build_aqi_report(database, 4946813, limit=10)
    )

    assert "location 4946813" in text
    assert "overall AQI" in text
    assert "pm25" in text
    assert "Stored window" in text
    assert "44.0" in text


def test_format_aqi_report_no_data():
    database = MagicMock()
    database.query_air_quality.return_value = []
    database.aggregate_aqi.return_value = []

    text = pipeline_main.format_aqi_report(
        pipeline_main.build_aqi_report(database, 4946813, limit=10)
    )

    assert "no stored air-quality data" in text


@pytest.mark.asyncio
async def test_main_report_flag_prints_table_and_skips_cycle(
    monkeypatch, capsys,
):
    monkeypatch.delenv("DATADOG_API_KEY", raising=False)
    database = MagicMock()
    database.quarantine_events = "the-collection"
    database.quarantine_retention_days = 7
    database.query_air_quality.return_value = [
        _stored_doc(
            4946813,
            {"pm25": 12.5, "pm25_unit": "µg/m³"},
            {"pm25_aqi": 52},
        )
    ]
    database.aggregate_aqi.return_value = []

    monkeypatch.setattr(pipeline_main, "MongoDBClient", lambda: database)

    await pipeline_main.main(["--report"])

    # The report path is read-only: it must NOT insert or run a cycle.
    database.insert_air_quality.assert_not_called()
    database.update_pipeline_status.assert_not_called()
    out = capsys.readouterr().out
    assert "overall AQI" in out
    assert "pm25" in out
    database.close.assert_called_once_with()


def test_parse_args_report_defaults():
    args = pipeline_main.parse_args(["--report"])
    assert args.report is True
    assert args.report_limit == 100


def test_parse_args_report_limit_override():
    args = pipeline_main.parse_args(["--report", "--report-limit", "250"])
    assert args.report is True
    assert args.report_limit == 250
