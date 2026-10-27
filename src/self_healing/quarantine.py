import inspect
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Callable
from uuid import uuid4

from loguru import logger

_MEMORY_MODE_WARNED = False


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

# Khai bao lop QuarantineManager de quan ly cac ban ghi bi cach ly
#
# Lifecycle (1 nguon ghi duy nhat, ket thuc viec 2 he API song song):
#   pending -> in_flight (claim CAS) -> replayed
#   pending -> in_flight (claim CAS) -> pending (con cap retry)
#   pending -> in_flight (claim CAS) -> failed (het cap, terminal)
#
# Claim "in_flight" co lease: worker crash giua replay se de lai
# in_flight, record co the duoc claim lai sau QUARANTINE_CLAIM_LEASE_SECONDS
# (mac dinh 600s) thay vi bi keo vao bienvan.
class QuarantineManager:
    def __init__(
        self,
        collection: Any = None,
        retention_days: int = 30,
        max_replay_attempts: int | None = None,
        claim_lease_seconds: int | None = None,
    ) -> None:
        if retention_days <= 0:
            raise ValueError("Retention days must be a positive integer.")
        if max_replay_attempts is None:
            max_replay_attempts = int(os.getenv("MAX_REPLAY_ATTEMPTS", "5"))
        if max_replay_attempts < 1:
            raise ValueError("max_replay_attempts must be >= 1.")
        if claim_lease_seconds is None:
            claim_lease_seconds = int(
                os.getenv("QUARANTINE_CLAIM_LEASE_SECONDS", "600")
            )
        if claim_lease_seconds < 1:
            raise ValueError("claim_lease_seconds must be >= 1.")
        self.collection = collection
        self.retention_days = retention_days
        self.max_replay_attempts = max_replay_attempts
        self.claim_lease_seconds = claim_lease_seconds
        self._memory_store: dict[str, dict[str, Any]] = {}
        if collection is None:
            global _MEMORY_MODE_WARNED
            if not _MEMORY_MODE_WARNED:
                # In-memory chi dung cho dev/test: record bi mat khi restart
                # process. Cho log de khang bao neu co ai de thua collection
                # o production (con duong 2.8, xem docs/timeline.md).
                # Warn 1 lan per process: unit tests construct nhat nhap
                # khong the spam nhat nhap duoc log.
                logger.warning(
                    "QuarantineManager has no MongoDB collection: "
                    "quarantine records live only in this process's "
                    "memory and are lost on restart. This must never "
                    "happen in production."
                )
                _MEMORY_MODE_WARNED = True
# Dua du lieu vao cach ly voi cac thong tin ve loi va metadata tuong ung.
# Tra ve mot dict chua thong tin cua ban ghi vua duoc them.
    def add_to_quarantine(self, data: Any, error: Exception | str, metadata: Optional[dict[str, Any]] = None) -> dict[str, Any]:
        now = _utcnow()
        record = {
            "quarantine_id": str(uuid4()),
            "data": data,
            "error": str(error),
            "metadata": metadata or {},
            "status": "pending",
            "created_at": now,
            "updated_at": now,
            "replay_attempts": 0,
            "expires_at": now + timedelta(days=self.retention_days),
        }
        if self.collection is not None:
            result = self.collection.insert_one(record.copy())
            if getattr(result, "inserted_id", None) is not None:
                record["_id"] = result.inserted_id
        else:
            self._memory_store[record["quarantine_id"]] = record.copy()
        return record
# Lay du lieu trong uarantine voi gioi han so luong ban ghi.
# Tra ve mot danh sach cac ban ghi.
    def replay(self, processor: Callable[[Any], Any], limit: int = 100) -> dict[str, int]:
        if limit < 1:
            raise ValueError("Limit must be a positive integer.")
        result = {"processed": 0, "succeeded": 0, "failed": 0}
        for record_id, data in self._pending_for_replay(limit):
            result["processed"] += 1
            try:
                processor(data)
            except Exception as exc:
                self._mark_replay_failed(record_id, exc)
                result["failed"] += 1
            else:
                self._mark_replayed(record_id)
                result["succeeded"] += 1
        return result

    async def replay_async(
        self,
        processor: Callable[[Any], Any],
        limit: int = 100,
    ) -> dict[str, int]:
        """Replay quarantined records through an async processor."""
        if limit < 1:
            raise ValueError("Limit must be a positive integer.")
        result = {"processed": 0, "succeeded": 0, "failed": 0}
        for record_id, data in self._pending_for_replay(limit):
            result["processed"] += 1
            try:
                outcome = processor(data)
                if inspect.isawaitable(outcome):
                    await outcome
            except Exception as exc:
                self._mark_replay_failed(record_id, exc)
                result["failed"] += 1
            else:
                self._mark_replayed(record_id)
                result["succeeded"] += 1
        return result

    def _pending_for_replay(self, limit: int) -> list[tuple[str, Any]]:
        """Claim pending records for replay, oldest first.

        A claim is CAS-guarded (``pending -> in_flight``), so two workers
        replaying the same batch cannot both claim the same record: the
        second claim sees the record is no longer ``pending`` and skips
        it. The dedup_key unique index on air_quality makes even a
        duplicate processor call a no-op, so a race at the processor
        cannot duplicate stored data.
        """
        claimed: list[tuple[str, Any]] = []
        now = _utcnow()
        for record in self._get_pending_records(limit):
            record_id = record["quarantine_id"]
            # Normal claims guard on ``pending``; reclaims of a stale lease
            # guard on ``in_flight`` (that is the only state a record read
            # from here can be in when it is not pending).
            expected = "pending" if record.get("status") == "pending" else "in_flight"
            won = self._update_record(
                record_id,
                expected_status=expected,
                increment={"replay_attempts": 1},
                status="in_flight",
                in_flight_at=now,
                updated_at=now,
            )
            if not won:
                # Another worker claimed this record between the read and
                # the claim; the next batch will pick it up.
                continue
            claimed.append((record_id, record["data"]))
        return claimed

    def _mark_replayed(self, record_id: str) -> bool:
        # Only ``in_flight -> replayed`` is allowed: a record another
        # worker has already parked as ``failed`` (replay cap reached)
        # must not be flipped back to ``replayed``.
        won = self._update_record(
            record_id,
            expected_status="in_flight",
            status="replayed",
            replayed_at=_utcnow(),
            updated_at=_utcnow(),
        )
        if not won:
            logger.warning(
                "Replay of {} lost the race to another worker; "
                "leaving the record state untouched.",
                record_id,
            )
        return won

    def _mark_replay_failed(self, record_id: str, error: Exception) -> bool:
        # ``_pending_for_replay`` already incremented replay_attempts when
        # the record was claimed, so the stored value reflects this attempt.
        attempts = self._attempts_for(record_id)
        status = "pending"
        if attempts >= self.max_replay_attempts:
            # Terminal: stop retrying a record that keeps failing so it does
            # not ping-pong forever. It stays in the store for inspection.
            status = "failed"
        won = self._update_record(
            record_id,
            expected_status="in_flight",
            status=status,
            last_replay_error=str(error),
            in_flight_at=None,
            updated_at=_utcnow(),
        )
        if not won:
            logger.warning(
                "Replay-failure marking of {} lost the race to another "
                "worker; leaving the record state untouched.",
                record_id,
            )
        return won

    def _attempts_for(self, record_id: str) -> int:
        if self.collection is not None:
            record = self.collection.find_one(
                {"quarantine_id": record_id}, {"replay_attempts": 1}
            )
            return int((record or {}).get("replay_attempts", 0))
        record = self._memory_store.get(record_id, {})
        return int(record.get("replay_attempts", 0))

    # Xoa cac ban ghi da het han trong uarantine.
    # Tra ve so luong ban ghi da xoa.
    def cleanup(self) -> int:
        now = _utcnow()
        if self.collection is not None:
            result = self.collection.delete_many(
                {
                    "expires_at": {"$lte": now},
                    "status": {"$in": ["pending", "replayed", "failed", "in_flight"]},
                }
            )
            return result.deleted_count
        expired = [
            record_id
            for record_id, record in self._memory_store.items()
            if record.get("expires_at") and record["expires_at"] <= now
            and record.get("status") in {"pending", "replayed", "failed", "in_flight"}
        ]
        for record_id in expired:
            del self._memory_store[record_id]
        return len(expired)

    def _stale_before(self) -> datetime:
        """In-flight claims older than this are considered abandoned."""
        return _utcnow() - timedelta(seconds=self.claim_lease_seconds)

    def _get_pending_records(self, limit: int) -> list[dict[str, Any]]:
        """Pending records, plus in-flight claims whose lease expired.

        Recovering expired leases is what keeps a worker crash from
        parking records in ``in_flight`` forever.
        """
        stale_before = self._stale_before()
        if self.collection is not None:
            cursor = self.collection.find(
                {
                    "$or": [
                        {"status": "pending"},
                        {
                            "status": "in_flight",
                            "in_flight_at": {"$lt": stale_before},
                        },
                    ]
                }
            ).sort("created_at", 1).limit(limit)
            return list(cursor)
        records = [
            record.copy()
            for record in self._memory_store.values()
            if record.get("status") == "pending"
            or (
                record.get("status") == "in_flight"
                and (record.get("in_flight_at") or _utcnow()) < stale_before
            )
        ]
        records.sort(key=lambda record: record["created_at"])
        return records[:limit]

    def _update_record(
        self,
        record_id: str,
        expected_status: Optional[str] = None,
        increment: Optional[dict[str, int]] = None,
        **fields: Any,
    ) -> bool:
        """Update one record, optionally guarded by its current status.

        With ``expected_status`` this is a compare-and-set: the update
        only applies when the record is still in that status, and the
        method returns whether the guard matched. Callers use the
        return value to detect a race with another worker instead of
        overwriting a state someone else already settled.
        """
        if self.collection is not None:
            filter_doc = {"quarantine_id": record_id}
            if expected_status is not None:
                filter_doc["status"] = expected_status
            update_doc: dict[str, Any] = {}
            if fields:
                update_doc["$set"] = fields
            if increment:
                update_doc["$inc"] = increment
            if not update_doc:
                return True
            result = self.collection.update_one(filter_doc, update_doc)
            return result.modified_count > 0
        record = self._memory_store.get(record_id)
        if record is None:
            return False
        if expected_status is not None and record.get("status") != expected_status:
            return False
        record.update(fields)
        if increment:
            for key, delta in increment.items():
                record[key] = record.get(key, 0) + delta
        return True
