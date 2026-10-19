from unittest.mock import MagicMock

import pytest

from src.database.mongodb import MongoDBClient
from src.fetcher.fetcher import DataFetcher
from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.validation.validator import DataValidator


class MockOpenAQClient:
    def __init__(self, timeout):
        self.timeout = timeout

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        _ = args
        return None

    async def get(self, url, **kwargs):
        assert "headers" in kwargs
        response = MagicMock()
        response.status_code = 200
        response.raise_for_status = MagicMock()
        if "/latest" in url:
            response.json.return_value = {
                "results": [
                    {
                        "value": 12.5,
                        "sensorsId": 701,
                        "datetime": {"utc": "2026-10-19T10:00:00Z"},
                    }
                ]
            }
        else:
            response.json.return_value = {
                "results": [
                    {
                        "parameter": {"name": "pm25", "units": "µg/m³"}
                    }
                ]
            }
        return response


@pytest.mark.asyncio
async def test_fetch_validate_and_persist_flow(monkeypatch):
    monkeypatch.setattr(
        "src.fetcher.fetcher.httpx.AsyncClient",
        MockOpenAQClient,
    )
    fetcher = DataFetcher()
    database = MagicMock(spec=MongoDBClient)
    pipeline = Pipeline(fetcher, DataValidator(), database)

    inserted_count = await pipeline.run_once({"location_id": 4946813})

    assert inserted_count == 1
    database.insert_air_quality.assert_called_once()
    document = database.insert_air_quality.call_args.args[0]
    assert document["metadata"] == {"location_id": 4946813}
    assert document["timestamp"].isoformat() == "2026-10-19T10:00:00+00:00"
    assert document["measurements"] == {
        "pm25": 12.5,
        "pm25_unit": "µg/m³",
    }
    assert document["aqi"]["pm25_aqi"] == 52
    assert document["event_id"]


@pytest.mark.asyncio
async def test_failed_fetch_is_not_silently_treated_as_success():
    fetcher = MagicMock(spec=DataFetcher)
    fetcher.fetch.return_value = None
    database = MagicMock(spec=MongoDBClient)
    pipeline = Pipeline(fetcher, DataValidator(), database)

    with pytest.raises(RuntimeError, match="fetch failed"):
        await pipeline.run_once()

    database.insert_air_quality.assert_not_called()


def test_self_healing_quarantines_invalid_data():
    engine = SelfHealingEngine()

    incident = engine.handle_error(
        ValueError("invalid measurement"),
        data={"value": -1},
        component="validation",
    )

    assert incident.retryable is False
    record = next(iter(engine.quarantine_manager._memory_store.values()))
    assert record["data"] == {"value": -1}
    assert record["status"] == "pending"


@pytest.mark.asyncio
async def test_pipeline_rejects_measurement_without_timestamp():
    fetcher = MagicMock(spec=DataFetcher)
    fetcher.fetch.return_value = {
        "results": [
            {
                "value": 12.5,
                "parameter_name": "pm25",
                "unit": "µg/m³",
                "datetime": {"utc": ""},
            }
        ]
    }
    database = MagicMock(spec=MongoDBClient)
    pipeline = Pipeline(fetcher, DataValidator(), database)

    with pytest.raises(ValueError, match="timestamp is required"):
        await pipeline.run_once()

    database.insert_air_quality.assert_not_called()
