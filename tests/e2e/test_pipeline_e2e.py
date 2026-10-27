"""E2E tests: real Pipeline + MongoDBClient + SelfHealingEngine, fake I/O.

The only two boundaries are faked:
- OpenAQ HTTP  -> httpx.MockTransport (the real DataFetcher code runs)
- MongoDB      -> an in-memory fake that enforces the real unique
                  ``dedup_key`` index by raising genuine
                  ``pymongo.errors.DuplicateKeyError``

Everything in between (retry loop, backoff, quarantine, replay, AQI
calculation, pipeline_status, dedup tolerance) is the production code.
Runs anywhere — no Atlas, no Docker — so CI can run these too.
"""

import json
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import httpx
import pytest

from src.database.mongodb import MongoDBClient
from src.fetcher.fetcher import DataFetcher
from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.self_healing.quarantine import QuarantineManager

LOCATION_ID = 4946813
VALID_TS = "2026-10-26T08:00:00Z"


# ---------------------------------------------------------------------------
# In-memory MongoDB fake
# ---------------------------------------------------------------------------


class _FakeCursor:
    def __init__(self, docs):
        self._docs = docs

    def sort(self, *_args, **_kwargs):
        return self

    def limit(self, n: int | None = None):
        self._docs = self._docs[:n] if n is not None else self._docs
        return self

    def __iter__(self):
        return iter(self._docs)


class _FakeAirQuality:
    """Enforces the unique dedup_key index like the real Atlas collection."""

    def __init__(self):
        self.documents: list[dict] = []
        self._dedup_keys: set[str] = set()

    def create_index(self, *args, **kwargs):
        return "index"

    def insert_one(self, doc: dict) -> SimpleNamespace:
        if doc["dedup_key"] in self._dedup_keys:
            raise _duplicate_key_error()
        self._dedup_keys.add(doc["dedup_key"])
        self.documents.append(doc)
        return SimpleNamespace(inserted_id=uuid.uuid4())

    def find(self, filters: dict | None = None):
        filters = filters or {}
        docs = [
            doc for doc in self.documents
            if all(doc.get(key) == value for key, value in filters.items())
        ]
        return _FakeCursor(docs)

    def aggregate(self, pipeline):
        return []


class _FakeQuarantine:
    def __init__(self):
        self.documents: list[dict] = []

    def create_index(self, *args, **kwargs):
        return "index"

    def insert_one(self, doc: dict) -> SimpleNamespace:
        self.documents.append(doc)
        return SimpleNamespace(inserted_id=uuid.uuid4())

    def find(self, filters: dict | None = None, *args, **kwargs):
        filters = filters or {}
        if "$or" in filters:
            or_filters = filters["$or"]
            docs = [d for d in self.documents if _matches_any(d, or_filters)]
        else:
            docs = [d for d in self.documents if _matches(d, filters)]
        docs.sort(key=lambda d: d.get("created_at", datetime.min))
        return _FakeCursor(docs)

    def find_one(self, filters: dict, projection=None):
        for doc in self.documents:
            if _matches(doc, filters):
                if projection:
                    return {k: doc.get(k) for k in projection}
                return doc
        return None

    def update_one(self, filters: dict, update: dict, **_kwargs):
        for doc in self.documents:
            if _matches(doc, filters):
                for key, value in update.get("$set", {}).items():
                    doc[key] = value
                for key, value in update.get("$inc", {}).items():
                    doc[key] = doc.get(key, 0) + value
                return SimpleNamespace(
                    matched_count=1, modified_count=1
                )
        return SimpleNamespace(matched_count=0, modified_count=0)

    def delete_many(self, filters: dict):
        before = len(self.documents)
        if "$or" in filters:
            or_filters = filters["$or"]
            self.documents = [
                d for d in self.documents if not _matches_any(d, or_filters)
            ]
        else:
            self.documents = [
                d for d in self.documents if not _matches(d, filters)
            ]
        return SimpleNamespace(deleted_count=before - len(self.documents))


class _FakePipelineStatus:
    def __init__(self):
        self.current: dict = {}
        self.calls: list[dict] = []

    def update_one(self, filter_, update, upsert=False, **_kwargs):
        record = self.current
        record.update(update.get("$set", {}))
        self.calls.append(
            {"status": update.get("$set", {}).get("status"), "upsert": upsert}
        )
        return SimpleNamespace(
            matched_count=1 if record else 0,
            upserted_id="current" if upsert else None,
        )


def _duplicate_key_error() -> Exception:
    from pymongo.errors import DuplicateKeyError

    return DuplicateKeyError(
        "E11000 duplicate key error collection: airquality.air_quality "
        "index: air_quality_dedup_idx dup key: { dedup_key: ... }"
    )


def _matches(doc: dict, filters: dict) -> bool:
    """Tiny Mongo filter matcher: equality, $or, $in, $lt, $lte, $set."""
    for key, condition in filters.items():
        if isinstance(condition, dict):
            if "$or" in condition:
                raise ValueError("use _matches_any instead of $or here")
            for op, value in condition.items():
                actual = doc.get(key)
                if op == "$in" and actual not in value:
                    return False
                if op == "$lt" and not (actual is not None and actual < value):
                    return False
                if op == "$lte" and not (actual is not None and actual <= value):
                    return False
        elif doc.get(key) != condition:
            return False
    return True


def _matches_any(doc: dict, or_filters: list[dict]) -> bool:
    return any(_matches(doc, sub) for sub in or_filters)


def build_database() -> tuple[MongoDBClient, dict]:
    fake_db = SimpleNamespace(
        __getitem__=lambda self=None, *args: None,
    )
    air_quality = _FakeAirQuality()
    quarantine = _FakeQuarantine()
    status = _FakePipelineStatus()

    class _DB:
        def __getitem__(self, name: str):
            return {
                "air_quality": air_quality,
                "quarantine_events": quarantine,
                "pipeline_status": status,
            }[name]

    class _Client:
        def __getitem__(self, name: str):
            return _DB()

    database = MongoDBClient(database_name="airquality", client=_Client())
    collections = {
        "air_quality": air_quality,
        "quarantine_events": quarantine,
        "pipeline_status": status,
    }
    _ = fake_db
    return database, collections


# ---------------------------------------------------------------------------
# OpenAQ mock (httpx boundary only)
# ---------------------------------------------------------------------------

LATEST_OK = {
    "results": [
        {
            "value": 12.5,
            "sensorsId": 101,
            "datetime": {"utc": VALID_TS},
        },
        {
            "value": 41.0,
            "sensorsId": 102,
            "datetime": {"utc": VALID_TS},
        },
    ]
}
def _json(payload) -> httpx.Response:
    return httpx.Response(
        200, json=payload, headers={"content-type": "application/json"}
    )


def make_openaq_client_factory(mode: str) -> callable:
    """Build an httpx.AsyncClient whose transport answers OpenAQ calls.

    mode:
      "ok"        - latest returns two measurements + sensor info
      "server5xx" - latest returns 503 (retryable)

    Any other mode raises, so new modes fail loudly in tests.
    """

    def _openaq_handler(request: httpx.Request) -> httpx.Response:
        url = str(request.url)
        if "/sensors/" in url:
            return _json(
                {"results": [{"parameter": {"name": "pm25", "units": "µg/m³"}}]}
            )
        if mode == "ok":
            return _json(LATEST_OK)
        if mode == "server5xx":
            return httpx.Response(503, json={"error": "upstream down"})
        raise ValueError(f"unknown mode {mode}")

    def _factory() -> httpx.AsyncClient:
        transport = httpx.MockTransport(_openaq_handler)
        return httpx.AsyncClient(transport=transport, timeout=1.0)

    return _factory


# ---------------------------------------------------------------------------
# Scenario 1: happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_fetch_validates_persists_and_reports_running():
    from src.main import run_cycle

    database, collections = build_database()
    fetcher = DataFetcher(client_factory=make_openaq_client_factory("ok"))
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=collections["quarantine_events"]
        ),
        max_retries=3,
    )
    pipeline = Pipeline(
        fetcher,
        _validator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
    )

    # run_cycle = the exact unit the scheduler executes: pipeline +
    # pipeline_status upsert in one go.
    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    assert result.status == "running"
    assert result.records_fetched == 2
    assert result.records_inserted == 2
    assert result.incident is None

    # Both measurements reached Mongo, each tagged with the sensor parameter.
    docs = collections["air_quality"].documents
    assert len(docs) == 2
    assert any("pm25_aqi" in doc["aqi"] for doc in docs)

    # The pipeline_status upsert happened with "running".
    assert collections["pipeline_status"].current["status"] == "running"


def _validator():
    from src.validation.validator import DataValidator

    return DataValidator()


# ---------------------------------------------------------------------------
# Scenario 2: OpenAQ 5xx for the whole run -> retry, then degrade + quarantine
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_server_errors_exhaust_retries_then_degrade_and_quarantine():
    database, collections = build_database()
    fetcher = DataFetcher(client_factory=make_openaq_client_factory("server5xx"))
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=collections["quarantine_events"]
        ),
        max_retries=3,
    )
    pipeline = Pipeline(
        fetcher,
        _validator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
    )

    from src.main import run_cycle

    result = await run_cycle(database, None, pipeline, LOCATION_ID)

    # Transient error retried 3 times, then degraded without crashing.
    assert result.status == "degraded"
    assert result.attempts == 4  # 1 initial + 3 retries
    assert result.records_inserted == 0
    assert result.incident is not None
    assert result.incident.error_type == "ConnectionError"

    # The single failed request was quarantined exactly once.
    pending = [
        d for d in collections["quarantine_events"].documents
        if d["status"] == "pending"
    ]
    assert len(pending) == 1
    assert pending[0]["data"] == {"location_id": LOCATION_ID}

    # Status record reflects the degradation and its incidents.
    assert collections["pipeline_status"].current["status"] == "degraded"
    incidents = collections["pipeline_status"].current["incidents"]
    assert len(incidents) >= 1


# ---------------------------------------------------------------------------
# Scenario 3: partial write, then replay -> NO duplicate documents
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_partial_write_then_replay_stores_no_duplicates():
    database, collections = build_database()
    fetcher = DataFetcher(client_factory=make_openaq_client_factory("ok"))
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=collections["quarantine_events"]
        ),
        max_retries=3,
    )
    pipeline = Pipeline(
        fetcher,
        _validator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
    )

    # First cycle: Mongo dies AFTER the first document is stored.
    real_insert = database.insert_air_quality
    state = {"count": 0}

    def flaky_insert(document):
        state["count"] += 1
        if state["count"] == 2:
            raise RuntimeError("Mongo connection lost mid-batch")
        return real_insert(document)

    with patch.object(database, "insert_air_quality", side_effect=flaky_insert):
        first = await pipeline.run_once({"location_id": LOCATION_ID})

    assert first.status == "degraded"
    assert len(collections["air_quality"].documents) == 1  # partial write
    assert len(
        [d for d in collections["quarantine_events"].documents
         if d["status"] == "pending"]
    ) == 1

    # Second cycle (a --replay or the next scheduled tick): the same
    # measurements come back from OpenAQ.
    second = await pipeline.run_once({"location_id": LOCATION_ID})

    assert second.status == "running"
    # Exactly one pm25 + one o3 document in total: the already-stored one
    # was skipped via the unique dedup_key, the missing one was added.
    docs = collections["air_quality"].documents
    assert len(docs) == 2
    dedup_keys = [doc["dedup_key"] for doc in docs]
    assert len(set(dedup_keys)) == 2


# ---------------------------------------------------------------------------
# Scenario 4 (bonus): replay marks the record replayed when it now succeeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_replay_marks_record_replayed_once_pipeline_recovers():
    database, collections = build_database()
    fetcher = DataFetcher(client_factory=make_openaq_client_factory("ok"))
    engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=collections["quarantine_events"],
            max_replay_attempts=5,
        ),
        max_retries=3,
    )
    pipeline = Pipeline(
        fetcher,
        _validator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=0,
    )

    # Seed a pending quarantine record the way handle_error() would.
    engine.quarantine_data(
        {"location_id": LOCATION_ID},
        ConnectionError("OpenAQ server error 503"),
    )

    async def processor(data):
        result = await pipeline.run_once(data)
        if result.status != "running" or result.records_inserted == 0:
            raise RuntimeError(f"replay did not persist (status={result.status})")

    summary = await engine.replay_quarantined_async(processor, limit=10)

    assert summary == {"processed": 1, "succeeded": 1, "failed": 0}
    record = collections["quarantine_events"].documents[0]
    assert record["status"] == "replayed"
    assert record["replayed_at"] is not None
