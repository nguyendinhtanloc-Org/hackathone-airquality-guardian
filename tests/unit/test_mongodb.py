from datetime import datetime, timezone
from unittest.mock import MagicMock, call, patch

import pytest
from pymongo.errors import DuplicateKeyError

from src.database.mongodb import MongoDBClient


@pytest.fixture
def mongo_client():
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    return MongoDBClient(
        database_name="test_airquality",
        client=client,
    ), client, collections


def _build_client(monkeypatch, data_retention_days: str):
    monkeypatch.setenv("DATA_RETENTION_DAYS", data_retention_days)
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
        "air_quality_dedup": MagicMock(),

    }
    database.__getitem__.side_effect = collections.__getitem__
    return MongoDBClient(database_name="test_airquality", client=client), collections



def test_initialization_creates_required_indexes(mongo_client):
    _, _, collections = mongo_client

    indexes = collections["air_quality"].create_index.call_args_list

    assert indexes[1] == call("metadata.location_id")

    dedup_indexes = collections[
        "air_quality_dedup"
    ].create_index.call_args_list

    assert call(
        [("dedup_key", 1)],
        unique=True,
        name="air_quality_dedup_key_idx",
    ) in dedup_indexes

    assert call(
        [("created_at", 1)],
        name="air_quality_dedup_created_at_idx",
    ) in dedup_indexes

    q_indexes = collections["quarantine_events"].create_index.call_args_list

    assert call(
        [("status", 1), ("created_at", 1)],
        name="pending_quarantine_idx",
    ) in q_indexes

    assert call(
        [("status", 1), ("in_flight_at", 1)],
        name="in_flight_quarantine_idx",
    ) in q_indexes

    assert call("expires_at") in q_indexes


def test_data_retention_creates_ttl_index_on_timestamp(monkeypatch):
    # Without a TTL, air_quality grows without bound on a long-running
    # deployment; DATA_RETENTION_DAYS finally drives one.
    _, collections = _build_client(monkeypatch, "30")
    indexes = collections["air_quality"].create_index.call_args_list
    assert indexes[0] == call("timestamp", expireAfterSeconds=30 * 86400)


def test_data_retention_zero_creates_plain_timestamp_index(monkeypatch):
    """0 means keep forever: no expireAfterSeconds, plain lookup index."""
    _, collections = _build_client(monkeypatch, "0")
    indexes = collections["air_quality"].create_index.call_args_list
    assert indexes[0] == call("timestamp")


def test_explicit_data_retention_days_param_wins_over_env(monkeypatch):
    monkeypatch.setenv("DATA_RETENTION_DAYS", "0")
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    MongoDBClient(
        database_name="test_airquality",
        client=client,
        data_retention_days=7,
    )
    assert collections["air_quality"].create_index.call_args_list[0] == call(
        "timestamp", expireAfterSeconds=7 * 86400
    )


def test_negative_data_retention_days_rejected(monkeypatch):
    monkeypatch.setenv("DATA_RETENTION_DAYS", "-1")
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    with pytest.raises(ValueError, match="DATA_RETENTION_DAYS must be >= 0"):
        MongoDBClient(database_name="test_airquality", client=client)


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
        "air_quality_dedup": MagicMock(),
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
    # The stored record is the caller's document plus a stable dedup_key.
    stored = collections["air_quality"].insert_one.call_args.args[0]
    assert stored["dedup_key"] == MongoDBClient._dedup_key(document)
    assert stored["metadata"] == document["metadata"]


def test_dedup_key_is_stable_for_the_same_observation():
    document = {
        "metadata": {"location_id": 123},
        "timestamp": datetime(2026, 10, 19, tzinfo=timezone.utc),
        "measurements": {"pm25": 12.5, "o3": 40.0},
        "aqi": {"pm25_aqi": 52},
    }
    other_event = dict(document, event_id="different-uuid")
    assert MongoDBClient._dedup_key(document) == MongoDBClient._dedup_key(other_event)


def test_dedup_key_differs_when_values_differ():
    base = {
        "metadata": {"location_id": 123},
        "timestamp": datetime(2026, 10, 19, tzinfo=timezone.utc),
        "measurements": {"pm25": 12.5},
    }
    assert MongoDBClient._dedup_key(base) != MongoDBClient._dedup_key(
        dict(base, measurements={"pm25": 13.0})
    )


def test_insert_air_quality_is_idempotent_on_duplicate(mongo_client):
    database_client, _, collections = mongo_client
    document = {
        "metadata": {"location_id": 123},
        "timestamp": datetime(2026, 10, 19, tzinfo=timezone.utc),
        "measurements": {"pm25": 12.5},
    }
    collections["air_quality"].insert_one.side_effect = DuplicateKeyError(
        "duplicate"
    )

    # No raise, no crash: a re-inserted observation is a no-op.
    assert database_client.insert_air_quality(document) is None


def _bare_database_client() -> tuple:
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    return MongoDBClient(database_name="test_airquality", client=client), collections


def test_quarantine_retention_days_follows_env_at_construction(monkeypatch):
    # Regression: retention was a class attribute read at import time, so
    # .env values were silently ignored. Now it is read per instance.
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "30")
    database_client, _ = _bare_database_client()
    assert database_client.quarantine_retention_days == 30

    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "2")
    database_client, _ = _bare_database_client()
    assert database_client.quarantine_retention_days == 2


def test_quarantine_retention_days_explicit_wins_over_env(monkeypatch):
    monkeypatch.setenv("QUARANTINE_RETENTION_DAYS", "30")
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    database_client = MongoDBClient(
        database_name="test_airquality",
        client=client,
        quarantine_retention_days=9,
    )
    assert database_client.quarantine_retention_days == 9


def test_quarantine_retention_days_must_be_positive():
    client = MagicMock()
    database = MagicMock()
    client.__getitem__.return_value = database
    collections = {
        "air_quality": MagicMock(),
        "air_quality_dedup": MagicMock(),
        "quarantine_events": MagicMock(),
        "pipeline_status": MagicMock(),
    }
    database.__getitem__.side_effect = collections.__getitem__
    with pytest.raises(ValueError, match="must be > 0"):
        MongoDBClient(
            database_name="test_airquality",
            client=client,
            quarantine_retention_days=0,
        )


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
    
    

def test_query_pending_quarantine_sorts_oldest_first_and_limits(
    mongo_client,
):
    database_client, _, collections = mongo_client
    collection = collections["quarantine_events"]

    expected = [
        {"event_id": "event-1", "status": "pending"},
        {"event_id": "event-2", "status": "pending"},
    ]

    cursor = MagicMock()
    cursor.sort.return_value.limit.return_value = expected
    collection.find.return_value = cursor

    result = database_client.query_pending_quarantine(limit=2)

    assert result == expected

    collection.find.assert_called_once_with({"status": "pending"})
    cursor.sort.assert_called_once_with("created_at", 1)
    cursor.sort.return_value.limit.assert_called_once_with(2)


def test_query_pending_quarantine_requires_positive_limit(mongo_client):
    database_client, _, collections = mongo_client

    with pytest.raises(
        ValueError,
        match="limit must be a positive integer",
    ):
        database_client.query_pending_quarantine(limit=0)

    collections["quarantine_events"].find.assert_not_called()


def test_mark_quarantine_replayed_updates_status(mongo_client):
    database_client, _, collections = mongo_client
    collection = collections["quarantine_events"]

    resolved_at = datetime(2026, 10, 25, tzinfo=timezone.utc)

    with patch("src.database.mongodb.datetime") as mock_datetime:
        mock_datetime.now.return_value = resolved_at

        database_client.mark_quarantine_replayed("event-1")

    collection.update_one.assert_called_once()

    query, update = collection.update_one.call_args.args

    assert query == {
        "event_id": "event-1",
        "status": "pending",
    }
    assert update["$set"]["status"] == "replayed"
    assert update["$set"]["resolved_at"] == resolved_at
    assert update["$inc"]["retry_count"] == 1
    collection = collections["quarantine_events"]

    resolved_at = datetime(2026, 10, 25, tzinfo=timezone.utc)
    database_client._now = lambda: resolved_at

    database_client.mark_quarantine_replayed("event-1")


def test_mark_quarantine_failed_updates_status_and_error(mongo_client):
    database_client, _, collections = mongo_client
    collection = collections["quarantine_events"]

    error_message = "Replay failed: timeout"
    resolved_at = datetime(2026, 10, 25, tzinfo=timezone.utc)
    database_client._now = lambda: resolved_at

    database_client.mark_quarantine_failed(
        "event-1",
        error_message=error_message,
    )

    collection.update_one.assert_called_once()

    query, update = collection.update_one.call_args.args

    assert query == {
    "event_id": "event-1",
    "status": "pending",
    }  
    assert update["$set"]["status"] == "failed"
    assert update["$set"]["error_message"] == error_message
    assert update["$set"]["resolved_at"] == resolved_at
    
def test_mark_quarantine_replayed_handles_missing_event(mongo_client):
    database_client, _, collections = mongo_client
    collection = collections["quarantine_events"]

    collection.update_one.return_value.matched_count = 0

    # Thay bằng exception hoặc kết quả mong đợi
    # theo contract của mark_quarantine_replayed.
    database_client.mark_quarantine_replayed("missing-event")

    collection.update_one.assert_called_once()