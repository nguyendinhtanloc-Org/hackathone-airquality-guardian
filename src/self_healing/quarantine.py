from datetime import datetime, timedelta, timezone
from typing import Any, Optional, Callable
from uuid import uuid4

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

# Khai bao lop QuarantineManager de quan ly cac file bi cach ly
class QuarantineManager:
    def __init__(self, collection: Any = None, retention_days: int = 30) -> None:
        if retention_days <= 0:
            raise ValueError("Retention days must be a positive integer.")
        self.collection = collection
        self.retention_days = retention_days
        self._memory_store: dict[str, dict[str, Any]] = {}
# Dua du lieu vao cach ly voi cac thong tin ve loi va metadata tuong ung. Tra ve mot dict chua thong tin cua ban ghi da duoc them vao cach ly.
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
# Lay du lieu trong uarantine voi gioi han so luong ban ghi. Tra ve mot danh sach cac ban ghi.
    def replay(self, processor: Callable[[Any], Any], limit: int = 100) -> dict[str, int]:
        if limit < 1:
            raise ValueError("Limit must be a positive integer.")
        records = self._get_pending_records(limit)
        result = {"processed": 0, "succeeded": 0, "failed": 0}
        for record in records:
            record_id = record["quarantine_id"]
            result["processed"] += 1
            attempts = record.get("replay_attempts", 0) + 1
            self._update_record(record_id, replay_attempts=attempts, update_at=_utcnow())
            try:
                processor(record["data"])
            except Exception as exc:
                self._update_record(record_id, status="pending", last_replay_error=str(exc), update_at=_utcnow())
                result["failed"] += 1
            else:
                self._update_record(record_id, status="replayed", replayed_at=_utcnow(), update_at=_utcnow())
                result["succeeded"] += 1
        return result
    
    # Xoa cac ban ghi da het han trong uarantine. Tra ve so luong ban ghi da xoa.
    def cleanup(self) -> int:
        now = _utcnow()
        if self.collection is not None:
            result = self.collection.delete_many(
                {
                    "expires_at": {"$lte": now},
                    "status": {"$in": ["pending", "replayed", "failed"]},
                }
            )
            return result.deleted_count
        expired = [
            record_id
            for record_id, record in self._memory_store.items()
            if record.get("expires_at") and record["expires_at"] <= now
            and record.get("status") in {"pending", "replayed", "failed"}
        ]
        for record_id in expired:
            del self._memory_store[record_id]
        return len(expired)
    
    def _get_pending_records(self, limit: int) -> list[dict[str, Any]]:
        if self.collection is not None:
            cursor = self.collection.find(
                {"status": "pending"}
            ).sort("created_at", 1).limit(limit)
            return list(cursor)
        records = [
            record.copy()
            for record in self._memory_store.values()
            if record.get("status") == "pending"
        ]
        records.sort(key=lambda record: record["created_at"])
        return records[:limit]
    def _update_record(self, record_id: str, **fields: Any) -> None:
        if self.collection is not None:
            self.collection.update_one(
                {"quarantine_id": record_id},
                {"$set": fields},
            )
            return
        record = self._memory_store.get(record_id)
        if record is not None:
            record.update(fields)