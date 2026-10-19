from datetime import datetime, timezone
from unittest.mock import MagicMock, call

import pytest

from src.database.mongodb import MongoDBClient


@pytest.fixture
def mongo_client():
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    return MongoDBClient(
        database_name="test_airquality",
        client=client,
    ), client, collections


def test_initialization_creates_required_indexes(mongo_client):
    _, _, collections = mongo_client

    collections["air_quality"].create_index.assert_has_calls(
        [call("timestamp"), call("metadata.location_id")]
    )


def test_initialization_uses_database_from_uri_when_env_database_is_missing(
    monkeypatch,
):
    monkeypatch.delenv("MONGODB_DATABASE", raising=False)
    client = MagicMock()
    default_database = MagicMock()
    default_database.name = "airquality"
    client.get_default_database.return_value = default_database
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__

    database_client = MongoDBClient(
        uri="mongodb+srv://example.mongodb.net/airquality",
        client=client,
    )

    client.get_default_database.assert_called_once_with(default=None)
    client.__getitem__.assert_called_once_with("airquality")
    assert database_client.database is database


def test_insert_air_quality_returns_inserted_id(mongo_client):
    database_client, _, collections = mongo_client
    document = {
        "metadata": {"location_id": 123},
        "timestamp": datetime(2026, 10, 19, tzinfo=timezone.utc),
        "measurements": {"pm25": 12.5},
    }
    collections["air_quality"].insert_one.return_value.inserted_id = "record-1"

    inserted_id = database_client.insert_air_quality(document)

    assert inserted_id == "record-1"
    collections["air_quality"].insert_one.assert_called_once_with(document)


def test_insert_quarantine_stores_pending_record(mongo_client):
    database_client, _, collections = mongo_client
    collections["quarantine_events"].insert_one.return_value.inserted_id = "q-1"
    raw_data = {"location_id": 123}

    inserted_id = database_client.insert_quarantine(
        raw_data,
        error_type="validation_failed",
        error_message="Missing measurement",
        event_id="event-1",
    )

    assert inserted_id == "q-1"
    record = collections["quarantine_events"].insert_one.call_args.args[0]
    assert record["event_id"] == "event-1"
    assert record["raw_data"] == raw_data
    assert record["error_type"] == "validation_failed"
    assert record["error_message"] == "Missing measurement"
    assert record["retry_count"] == 0
    assert record["status"] == "pending"
    assert record["resolved_at"] is None
    assert record["created_at"].tzinfo == timezone.utc


def test_update_pipeline_status_upserts_status_and_metrics(mongo_client):
    database_client, _, collections = mongo_client
    metrics = {"records_valid": 10}
    incidents = [{"error": "temporary"}]
    last_fetch = datetime(2026, 10, 19, tzinfo=timezone.utc)

    database_client.update_pipeline_status(
        "degraded",
        metrics=metrics,
        incidents=incidents,
        last_fetch=last_fetch,
    )

    collections["pipeline_status"].update_one.assert_called_once_with(
        {"_id": "current"},
        {
            "$set": {
                "status": "degraded",
                "last_fetch": last_fetch,
                "metrics": metrics,
                "incidents": incidents,
            }
        },
        upsert=True,
    )


def test_update_pipeline_status_rejects_unknown_status(mongo_client):
    database_client, _, collections = mongo_client

    with pytest.raises(ValueError, match="status must be one of"):
        database_client.update_pipeline_status("broken")

    collections["pipeline_status"].update_one.assert_not_called()


def test_query_air_quality_sorts_newest_first_and_limits(mongo_client):
    database_client, _, collections = mongo_client
    cursor = MagicMock()
    expected = [{"timestamp": datetime(2026, 10, 19, tzinfo=timezone.utc)}]
    cursor.sort.return_value.limit.return_value = expected
    collections["air_quality"].find.return_value = cursor
    filters = {"metadata.location_id": 123}

    result = database_client.query_air_quality(filters, limit=5)

    assert result == expected
    collections["air_quality"].find.assert_called_once_with(filters)
    cursor.sort.assert_called_once_with("timestamp", -1)
    cursor.sort.return_value.limit.assert_called_once_with(5)


def test_query_air_quality_requires_positive_limit(mongo_client):
    database_client, _, collections = mongo_client

    with pytest.raises(ValueError, match="limit must be a positive integer"):
        database_client.query_air_quality(limit=0)

    collections["air_quality"].find.assert_not_called()


def test_aggregate_aqi_matches_filter_and_returns_summary(mongo_client):
    database_client, _, collections = mongo_client
    expected = [{"_id": 123, "average_aqi": 42.0, "max_aqi": 50, "min_aqi": 34, "records": 2}]
    collections["air_quality"].aggregate.return_value = expected
    filters = {"metadata.location_id": 123}

    result = database_client.aggregate_aqi(filters)

    assert result == expected
    pipeline = collections["air_quality"].aggregate.call_args.args[0]
    assert pipeline[0] == {"$match": filters}
    assert pipeline[-2] == {
        "$group": {
            "_id": "$location_id",
            "average_aqi": {"$avg": "$aqi"},
            "max_aqi": {"$max": "$aqi"},
            "min_aqi": {"$min": "$aqi"},
            "records": {"$sum": 1},
        }
    }


def test_close_closes_mongo_client(mongo_client):
    database_client, client, _ = mongo_client

    database_client.close()

    client.close.assert_called_once_with()
