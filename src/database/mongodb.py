"""MongoDB persistence for air-quality, quarantine, and pipeline records."""

import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import uuid4

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from pymongo.collection import Collection
from pymongo.database import Database


class MongoDBClient:
    """Provide access to the project's MongoDB collections."""

    AIR_QUALITY_COLLECTION = "air_quality"
    QUARANTINE_COLLECTION = "quarantine_events"
    PIPELINE_STATUS_COLLECTION = "pipeline_status"
    PIPELINE_STATUSES = {"running", "degraded", "stopped"}
    QUARANTINE_RETENTION_DAYS = int(
        os.getenv("QUARANTINE_RETENTION_DAYS", "7")
    )

    def __init__(
        self,
        uri: str | None = None,
        database_name: str | None = None,
        client: MongoClient[dict[str, Any]] | None = None,
    ) -> None:
        if uri is None or database_name is None:
            load_dotenv()

        if client is None:
            uri = uri or os.getenv("MONGODB_URI")
            if not uri:
                raise ValueError("MONGODB_URI must be configured.")
            client = MongoClient(uri)

        database_name = database_name or os.getenv("MONGODB_DATABASE")
        if database_name is None:
            default_database = client.get_default_database(default=None)
            if default_database is not None:
                database_name = default_database.name
        if not database_name:
            client.close()
            raise ValueError(
                "Set MONGODB_DATABASE or include a database name in MONGODB_URI."
            )

        self.client = client
        self.database: Database[dict[str, Any]] = client[database_name]
        self.air_quality: Collection[dict[str, Any]] = self.database[
            self.AIR_QUALITY_COLLECTION
        ]
        self.quarantine_events: Collection[dict[str, Any]] = self.database[
            self.QUARANTINE_COLLECTION
        ]
        self.pipeline_status: Collection[dict[str, Any]] = self.database[
            self.PIPELINE_STATUS_COLLECTION
        ]
        self._create_indexes()

    def _create_indexes(self) -> None:
        self.air_quality.create_index("timestamp")
        self.air_quality.create_index("metadata.location_id")

    def insert_air_quality(self, document: Mapping[str, Any]) -> Any:
        """Insert one air-quality document and return its inserted ID."""
        result = self.air_quality.insert_one(dict(document))
        return result.inserted_id

    def insert_quarantine(
        self,
        raw_data: Mapping[str, Any],
        error_type: str,
        error_message: str,
        event_id: str | None = None,
    ) -> Any:
        """Store a failed record for later inspection or replay.

        The record carries both the legacy fields and the fields
        ``QuarantineManager`` reads (quarantine_id, data, status,
        expires_at, replay_attempts) so a record written here can be
        replayed by ``python -m src.main --replay`` without any
        conversion step.
        """
        now = datetime.now(timezone.utc)
        record = {
            "event_id": event_id or str(uuid4()),
            "raw_data": dict(raw_data),
            "error_type": error_type,
            "error_message": error_message,
            "retry_count": 0,
            "created_at": now,
            "status": "pending",
            "resolved_at": None,
            # Fields owned by QuarantineManager.
            "quarantine_id": str(uuid4()),
            "data": dict(raw_data),
            "error": error_message,
            "metadata": {"error_type": error_type},
            "updated_at": now,
            "replay_attempts": 0,
            "expires_at": now + timedelta(days=self.QUARANTINE_RETENTION_DAYS),
        }
        result = self.quarantine_events.insert_one(record)
        return result.inserted_id

    def update_pipeline_status(
        self,
        status: str,
        metrics: Mapping[str, Any] | None = None,
        incidents: list[Mapping[str, Any]] | None = None,
        last_fetch: datetime | None = None,
    ) -> Any:
        """Upsert the current pipeline status without replacing omitted fields."""
        if status not in self.PIPELINE_STATUSES:
            raise ValueError(
                f"status must be one of {sorted(self.PIPELINE_STATUSES)}."
            )

        updates: dict[str, Any] = {
            "status": status,
            "last_fetch": last_fetch or datetime.now(timezone.utc),
        }
        if metrics is not None:
            updates["metrics"] = dict(metrics)
        if incidents is not None:
            updates["incidents"] = [dict(incident) for incident in incidents]

        return self.pipeline_status.update_one(
            {"_id": "current"},
            {"$set": updates},
            upsert=True,
        )

    def query_air_quality(
        self,
        filters: Mapping[str, Any] | None = None,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Return newest matching air-quality documents, up to ``limit``."""
        if limit < 1:
            raise ValueError("limit must be a positive integer.")
        cursor = self.air_quality.find(dict(filters or {}))
        return list(cursor.sort("timestamp", DESCENDING).limit(limit))

    def aggregate_aqi(
        self,
        filters: Mapping[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Summarize each location's per-record AQI values.

        The documented schema stores pollutant AQIs under keys such as
        ``aqi.pm25_aqi`` and ``aqi.o3_aqi``. Each record contributes its
        highest numeric pollutant AQI to the location-level summary.
        """
        pipeline: list[dict[str, Any]] = []
        if filters:
            pipeline.append({"$match": dict(filters)})

        pipeline.extend(
            [
                {
                    "$project": {
                        "location_id": "$metadata.location_id",
                        "aqi_values": {
                            "$map": {
                                "input": {
                                    "$filter": {
                                        "input": {
                                            "$objectToArray": {
                                                "$ifNull": ["$aqi", {}]
                                            }
                                        },
                                        "as": "entry",
                                        "cond": {
                                            "$and": [
                                                {
                                                    "$regexMatch": {
                                                        "input": "$$entry.k",
                                                        "regex": "_aqi$",
                                                    }
                                                },
                                                {"$isNumber": "$$entry.v"},
                                            ]
                                        },
                                    }
                                },
                                "as": "entry",
                                "in": "$$entry.v",
                            }
                        },
                    }
                },
                {
                    "$project": {
                        "location_id": 1,
                        "aqi": {"$max": "$aqi_values"},
                    }
                },
                {"$match": {"aqi": {"$ne": None}}},
                {
                    "$group": {
                        "_id": "$location_id",
                        "average_aqi": {"$avg": "$aqi"},
                        "max_aqi": {"$max": "$aqi"},
                        "min_aqi": {"$min": "$aqi"},
                        "records": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
            ]
        )
        return list(self.air_quality.aggregate(pipeline))

    def close(self) -> None:
        """Close the underlying MongoDB client."""
        self.client.close()
