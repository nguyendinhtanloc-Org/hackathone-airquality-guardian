"""MongoDB persistence for air-quality, quarantine, and pipeline records."""

import hashlib
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from uuid import uuid4

from dotenv import load_dotenv
from pymongo import DESCENDING, MongoClient
from pymongo.errors import DuplicateKeyError
from pymongo.collection import Collection
from pymongo.database import Database


class MongoDBClient:
    """Provide access to the project's MongoDB collections."""

    AIR_QUALITY_COLLECTION = "air_quality"
    QUARANTINE_COLLECTION = "quarantine_events"
    PIPELINE_STATUS_COLLECTION = "pipeline_status"
    PIPELINE_STATUSES = {"running", "degraded", "stopped"}
    DEDUP_COLLECTION = "air_quality_dedup"


    def __init__(
        self,
        uri: str | None = None,
        database_name: str | None = None,
        client: MongoClient[dict[str, Any]] | None = None,
        quarantine_retention_days: int | None = None,
        data_retention_days: int | None = None,
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
            # MONGODB_DATABASE_NAME is the variable the Atlas onboarding
            # console prints into .env; treat it as an alias so a copy-
            # paste from Atlas works without editing variable names.
            database_name = os.getenv("MONGODB_DATABASE_NAME")
        if database_name is None:
            default_database = client.get_default_database(default=None)
            if default_database is not None:
                database_name = default_database.name
        if not database_name:
            client.close()
            raise ValueError(
                "Set MONGODB_DATABASE (or MONGODB_DATABASE_NAME) or "
                "include a database name in MONGODB_URI."
            )

        # Read at construction time (after load_dotenv may have run), NOT at
        # import time: a class attribute captured os.getenv() once, before
        # .env was loaded, so .env values were silently ignored.
        self.quarantine_retention_days = (
            quarantine_retention_days
            if quarantine_retention_days is not None
            else int(os.getenv("QUARANTINE_RETENTION_DAYS", "7"))
        )
        if self.quarantine_retention_days <= 0:
            raise ValueError("QUARANTINE_RETENTION_DAYS must be > 0.")
        # 0 means "keep forever" (no TTL index); otherwise air_quality
        # documents expire that many days after their timestamp.
        self.data_retention_days = (
            data_retention_days
            if data_retention_days is not None
            else int(os.getenv("DATA_RETENTION_DAYS", "30"))
        )
        if self.data_retention_days < 0:
            raise ValueError("DATA_RETENTION_DAYS must be >= 0.")

        self.client = client
        self.database: Database[dict[str, Any]] = client[database_name]
        self.air_quality: Collection[dict[str, Any]] = self.database[
            self.AIR_QUALITY_COLLECTION
        ]
        self.air_quality_dedup = self.database[
            self.DEDUP_COLLECTION
        ]
        self.quarantine_events: Collection[dict[str, Any]] = self.database[
            self.QUARANTINE_COLLECTION
        ]
        self.pipeline_status: Collection[dict[str, Any]] = self.database[
            self.PIPELINE_STATUS_COLLECTION
        ]
        # If air_quality is a time-series collection (2.10, as created on
        # Atlas), the server REJECTS a plain TTL index on the timeField.
        # A TTL on a time-series collection that has a metaField must be
        # scoped to one meta value via partialFilterExpression. Detect the
        # actual layout up front so index creation below issues a command
        # the server accepts instead of crashing startup.
        (
            self._air_quality_is_timeseries,
            self._air_quality_meta_field,
        ) = self._detect_air_quality_timeseries()
        self._create_indexes()

    def _detect_air_quality_timeseries(self) -> tuple[bool, str | None]:
        """Return (is_time_series, meta_field) for air_quality.

        Best-effort: if the server will not answer listCollections we fall
        back to the regular-collection layout (indexes as before) instead
        of blocking startup.
        """
        try:
            info = next(
                self.database.list_collections(
                    filter={"name": self.AIR_QUALITY_COLLECTION}
                ),
                None,
            )
        except Exception:
            return False, None
        # Guard: real descriptors are dicts; mocks/None/other types fall to
        # the regular-collection path so tests with MagicMock databases keep
        # their original index-creation assertions intact.
        if not isinstance(info, Mapping):
            return False, None
        timeseries = (info.get("options") or {}).get("timeseries")
        if not timeseries:
            return False, None
        return True, timeseries.get("metaField")

    @staticmethod
    def _ttl_meta_value() -> Any:
        """Meta value the time-series TTL reaps.

        A meta-scoped TTL index only applies to documents of that meta
        value, and the pipeline monitors a single location by default, so
        the value is configured (AIR_QUALITY_TTL_META) instead of guessed.
        """
        raw = os.getenv("AIR_QUALITY_TTL_META", "4946813")
        try:
            return int(raw)
        except ValueError:
            return raw
    def _now(self) -> datetime:
        """Return the current UTC time."""
        return datetime.now(timezone.utc)
    @staticmethod
    def _dedup_key(document: Mapping[str, Any]) -> str:
        """Stable identity for one measurement observation.

        The same physical reading re-emitted by OpenAQ or replayed by
        ``--replay`` must map to the same key. ``measurements`` is a
        dynamic sub-document (pm25, o3, no2, ...), so we hash the whole
        document body instead of declaring a fixed compound index.
        """
        raw = {
            key: value
            for key, value in document.items()
            if key in {"metadata", "timestamp", "measurements", "aqi"}
        }
        serialized = json.dumps(
            raw,
            sort_keys=True,
            default=str,
            ensure_ascii=False,
        )
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def _create_indexes(self) -> None:
        if self.data_retention_days > 0:
            if self._air_quality_is_timeseries and self._air_quality_meta_field:
                # A TTL on a time-series collection requires a
                # partialFilterExpression on the metaField; index the
                # monitored meta value so the reaper actually prunes.
                self.air_quality.create_index(
                    "timestamp",
                    expireAfterSeconds=self.data_retention_days * 86400,
                    name="air_quality_ttl_idx",
                    partialFilterExpression={
                        self._air_quality_meta_field: self._ttl_meta_value()
                    },
                )
            else:
                # TTL index: MongoDB's background reaper deletes documents
                # whose timestamp is older than the expiry, which is what
                # keeps air_quality from growing without bound.
                self.air_quality.create_index(
                    "timestamp",
                    expireAfterSeconds=self.data_retention_days * 86400,
                )
        else:
            self.air_quality.create_index("timestamp")
        if not self._air_quality_is_timeseries:
            # A time-series collection may only be indexed on its
            # time/meta fields (the control indexes cover lookups), so the
            # regular meta index is created only for a regular collection.
            self.air_quality.create_index("metadata.location_id")
        # Time-series collections do not support this unique index.
    # Enforce deduplication in a regular collection instead.
        self.air_quality_dedup.create_index(
            [("dedup_key", 1)],
            unique=True,
            name="air_quality_dedup_key_idx",
        )

        # Markers are written per observation and otherwise never deleted
        # (air_quality's TTL reaper cannot touch a separate collection), so
        # the dedup store would grow without bound on a long-running
        # deployment. Align its TTL with the observations it dedups: same
        # horizon, 0 = keep forever (plain index, mirrors air_quality).
        if self.data_retention_days > 0:
            self.air_quality_dedup.create_index(
                "created_at",
                expireAfterSeconds=self.data_retention_days * 86400,
                name="air_quality_dedup_created_at_idx",
            )
        else:
            self.air_quality_dedup.create_index(
                [("created_at", 1)],
                name="air_quality_dedup_created_at_idx",
            )
        self.quarantine_events.create_index(
            [("status", 1), ("created_at", 1)],
            name="pending_quarantine_idx",
        )
        # The claim/lease query filters in_flight claims by in_flight_at
        # (reclaiming crashed workers); without this it collection-scans
        # once the store grows.
        self.quarantine_events.create_index(
            [("status", 1), ("in_flight_at", 1)],
            name="in_flight_quarantine_idx",
        )
        # cleanup() prunes expired records each cycle.
        self.quarantine_events.create_index("expires_at")
#đưa dữ liệu vào db, trả về id của bản ghi vừa được thêm vào, collection: air uality
    
    def insert_air_quality(self, document: Mapping[str, Any]) -> Any:
        """Insert a time-series observation with deduplication."""
        record = dict(document)
        metadata = record.get("metadata") or {}
        location_id = metadata.get("location_id")

        if location_id is not None:
            record.setdefault("station_id", location_id)

        dedup_key = self._dedup_key(record)
        record["dedup_key"] = dedup_key

        # The unique key lives in a regular collection because
        # air_quality is a MongoDB time-series collection (which cannot
        # carry a unique index). marker-first dedup:
        #   1. insert the marker -> the unique index on air_quality_dedup
        #      is the actual gate that stops a re-emitted or replayed
        #      observation from duplicating data.
        #   2. insert the observation into the time-series collection.
        #   3. if the time-series write fails, roll the marker back so the
        #      next cycle can retry instead of being silently blocked.
        marker = {
            "dedup_key": dedup_key,
            "created_at": self._now(),
            "station_id": record.get("station_id"),
            "timestamp": record.get("timestamp"),
        }
        try:
            self.air_quality_dedup.insert_one(marker)
        except DuplicateKeyError:
            # Observation already stored -> idempotent no-op.
            return None

        try:
            result = self.air_quality.insert_one(record)
        except Exception:
            self.air_quality_dedup.delete_one({"dedup_key": dedup_key})
            raise
        return result.inserted_id

#đưa dữ liệu vào db, trả về id của bản ghi vừa được thêm vào, collection: quarantine
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
            "expires_at": now + timedelta(days=self.quarantine_retention_days),
        }
        result = self.quarantine_events.insert_one(record)
        return result.inserted_id
#lưu trạng thái running PIPELINE_STATUSES
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

    
    def query_pending_quarantine(
        self, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return oldest pending quarantine events first."""
        if limit < 1:
            raise ValueError("limit must be a positive integer.")

        cursor = (
            self.quarantine_events
            .find({"status": "pending"})
            .sort("created_at", 1)
            .limit(limit)
        )
        return list(cursor)


    def mark_quarantine_replayed(
        self, event_id: str
    ) -> bool:
        """Mark a successfully replayed event."""
        result = self.quarantine_events.update_one(
            {"event_id": event_id, "status": "pending"},
            {
                "$set": {
                    "status": "replayed",
                    "resolved_at": self._now(),
                },
                "$inc": {"retry_count": 1},
            },
            
        )
        return result.modified_count == 1


    
    def mark_quarantine_failed(
        self, event_id: str, error_message: str
    ) -> bool:
        """Record a replay failure and retain the event for inspection."""
        result = self.quarantine_events.update_one(
            {"event_id": event_id, "status": "pending"},
            {
                "$set": {
                    "status": "failed",
                    "error_message": error_message,
                    "resolved_at": self._now(),
                },
                "$inc": {"retry_count": 1},
            },
        )
        return result.modified_count == 1

    def close(self) -> None:
        """Close the underlying MongoDB client."""
        self.client.close()