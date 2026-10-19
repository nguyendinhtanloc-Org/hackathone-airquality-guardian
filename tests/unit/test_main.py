from unittest.mock import AsyncMock, MagicMock

import pytest

from src import main as pipeline_main


@pytest.mark.asyncio
async def test_main_prints_run_summary_and_sends_datadog_telemetry(
    monkeypatch,
):
    monkeypatch.setenv("DATADOG_API_KEY", "test-api-key")
    database = MagicMock()
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

    await pipeline_main.main()

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
