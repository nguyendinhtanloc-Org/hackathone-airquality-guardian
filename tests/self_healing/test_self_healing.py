Ok bà, vậy mình triển khai luôn Ngày 2 nhé. Trong `src/self_healing/` hiện chỉ có `__init__.py`, mình sẽ tạo hai file mới và bộ unit tests, giữ nguyên pipeline Ngày 1.

Thiết kế dưới đây dùng MongoDB collection `quarantine_events` mà bà đã setup, hỗ trợ retry có giới hạn, cách ly dữ liệu và replay có kiểm soát. Các unit tests sẽ dùng mock nên không cần kết nối Atlas để chạy.

## 1. Tạo `src/self_healing/quarantine.py`

Trong PowerShell, bà đang ở thư mục `src`, chạy:

powershell
```
@'
"""MongoDB-backed quarantine management."""

from datetime import datetime, timedelta, timezone
from typing import Any, Callable, Optional
from uuid import uuid4

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

class QuarantineManager:
    """Store failed data and manage replay/cleanup lifecycle."""

    def __init__(
        self,
        collection: Any = None,
        retention_days: int = 30,
    ) -> None:
        if retention_days < 1:
            raise ValueError("retention_days must be >= 1")

        self.collection = collection
        self.retention_days = retention_days
        self._memory_store: dict[str, dict[str, Any]] = {}

    def add_to_quarantine(
        self,
        data: Any,
        error: Exception | str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Save failed data and return its quarantine record."""
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

    def replay(
        self,
        processor: Callable[[Any], Any],
        limit: int = 100,
    ) -> dict[str, int]:
        """Replay pending records, marking each success or failure."""
        if limit < 1:
            raise ValueError("limit must be >= 1")

        records = self._get_pending_records(limit)
        result = {"processed": 0, "succeeded": 0, "failed": 0}

        for record in records:
            record_id = record["quarantine_id"]
            result["processed"] += 1
            attempts = record.get("replay_attempts", 0) + 1

            self._update_record(
                record_id,
                replay_attempts=attempts,
                updated_at=_utcnow(),
            )

            try:
                processor(record["data"])
            except Exception as exc:
                self._update_record(
                    record_id,
                    status="pending",
                    last_replay_error=str(exc),
                    updated_at=_utcnow(),
                )
                result["failed"] += 1
            else:
                self._update_record(
                    record_id,
                    status="replayed",
                    last_replay_error=None,
                    replayed_at=_utcnow(),
                    updated_at=_utcnow(),
                )
                result["succeeded"] += 1

        return result

    def cleanup(self) -> int:
        """Remove expired records; return the number removed."""
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
'@ | Set-Content -Encoding utf8 .\self_healing\quarantine.py
```

File này có hai chế độ: truyền MongoDB collection để lưu thật, hoặc không truyền để dùng bộ nhớ cho unit tests và phát triển.

## 2. Tạo `src/self_healing/engine.py`

Engine sẽ phân loại lỗi theo khả năng retry. Lỗi tạm thời như timeout có thể retry; lỗi dữ liệu không hợp lệ sẽ được đưa vào quarantine thay vì retry vô ích.

Chạy tiếp lệnh này trong thư mục `src`:

powershell
```
@'
"""Self-healing orchestration for the air-quality pipeline."""

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional

from .quarantine import QuarantineManager

def _utcnow() -> datetime:
    return datetime.now(timezone.utc)

@dataclass
class Incident:
    """Information about a pipeline failure."""

    error: str
    error_type: str
    component: str = "pipeline"
    retryable: bool = False
    attempt: int = 0
    max_retries: int = 3
    incident_id: Optional[str] = None
    created_at: datetime = field(default_factory=_utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

@dataclass
class QuarantinedData:
    """Reference to data that has been isolated for later replay."""

    quarantine_id: str
    data: Any
    error: str
    status: str = "pending"
    created_at: datetime = field(default_factory=_utcnow)
    metadata: dict[str, Any] = field(default_factory=dict)

class SelfHealingEngine:
    """Decide whether to retry, quarantine, or replay failed data."""

    DEFAULT_RETRYABLE_ERRORS = (
        TimeoutError,
        ConnectionError,
    )

    def __init__(
        self,
        quarantine_manager: Optional[QuarantineManager] = None,
        max_retries: int = 3,
        retryable_errors: Optional[tuple[type[Exception], ...]] = None,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")

        self.quarantine_manager = (
            quarantine_manager or QuarantineManager()
        )
        self.max_retries = max_retries
        self.retryable_errors = (
            retryable_errors or self.DEFAULT_RETRYABLE_ERRORS
        )
        self.incidents: list[Incident] = []

    def handle_error(
        self,
        error: Exception,
        data: Any = None,
        component: str = "pipeline",
        attempt: int = 0,
        metadata: Optional[dict[str, Any]] = None,
    ) -> Incident:
        """Record an incident and quarantine data when retry is unsuitable."""
        if attempt < 0:
            raise ValueError("attempt must be >= 0")

        retryable = isinstance(error, self.retryable_errors)

        incident = Incident(
            error=str(error),
            error_type=type(error).__name__,
            component=component,
            retryable=retryable,
            attempt=attempt,
            max_retries=self.max_retries,
            metadata=metadata or {},
        )
        self.incidents.append(incident)

        if not self.should_retry(incident):
            self.quarantine_data(
                data=data,
                error=error,
                metadata={
                    **incident.metadata,
                    "component": component,
                    "incident_id": incident.incident_id,
                    "error_type": incident.error_type,
                },
            )

        return incident

    def should_retry(
        self,
        incident: Incident,
        attempt: Optional[int] = None,
    ) -> bool:
        """Return True only for retryable errors within the retry limit.

        attempt is zero-based: attempt=0 means the first failure.
        max_retries=3 permits retries after failures 0, 1, and 2.
        """
        current_attempt = incident.attempt if attempt is None else attempt

        return (
            incident.retryable
            and current_attempt < incident.max_retries
        )

    def quarantine_data(
        self,
        data: Any,
        error: Exception | str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> QuarantinedData:
        """Isolate failed data using the configured manager."""
        record = self.quarantine_manager.add_to_quarantine(
            data=data,
            error=error,
            metadata=metadata,
        )
        return QuarantinedData(
            quarantine_id=record["quarantine_id"],
            data=data,
            error=str(error),
            status=record.get("status", "pending"),
            created_at=record.get("created_at", _utcnow()),
            metadata=record.get("metadata", {}),
        )

    def replay_quarantined(
        self,
        processor: Any,
        limit: int = 100,
    ) -> dict[str, int]:
        """Replay quarantined records through the supplied processor."""
        return self.quarantine_manager.replay(
            processor=processor,
            limit=limit,
        )
'@ | Set-Content -Encoding utf8 .\self_healing\engine.py
```

Lưu ý: `should_retry()` chỉ quyết định có nên retry hay không; việc gọi lại thao tác pipeline cần do caller thực hiện. `replay_quarantined()` nhận một hàm `processor` để xử lý lại từng record.

## 3. Viết unit tests

Tạo file `tests/test_self_healing.py`. Vì terminal đang ở thư mục `src`, chạy:

powershell
```
@'
import pytest

from self_healing.engine import (
    Incident,
    QuarantinedData,
    SelfHealingEngine,
)
from self_healing.quarantine import QuarantineManager

def test_should_retry_transient_error():
    engine = SelfHealingEngine(max_retries=3)
    incident = Incident(
        error="request timed out",
        error_type="TimeoutError",
        retryable=True,
        attempt=0,
        max_retries=3,
    )

    assert engine.should_retry(incident) is True
    assert engine.should_retry(incident, attempt=2) is True
    assert engine.should_retry(incident, attempt=3) is False

def test_should_not_retry_permanent_error():
    engine = SelfHealingEngine()
    incident = Incident(
        error="invalid measurement",
        error_type="ValueError",
        retryable=False,
        attempt=0,
        max_retries=3,
    )

    assert engine.should_retry(incident) is False

def test_handle_error_quarantines_non_retryable_data():
    manager = QuarantineManager()
    engine = SelfHealingEngine(quarantine_manager=manager)
    data = {"sensor_id": "sensor-1", "pm25": -10}

    incident = engine.handle_error(
        ValueError("invalid measurement"),
        data=data,
        component="validation",
    )

    assert incident.retryable is False
    assert len(manager._memory_store) == 1

    record = next(iter(manager._memory_store.values()))
    assert record["data"] == data
    assert record["status"] == "pending"
    assert record["error"] == "invalid measurement"

def test_handle_error_does_not_quarantine_retryable_error():
    manager = QuarantineManager()
    engine = SelfHealingEngine(quarantine_manager=manager)

    incident = engine.handle_error(
        TimeoutError("temporary network issue"),
        data={"pm25": 20},
        attempt=0,
    )

    assert incident.retryable is True
    assert engine.should_retry(incident) is True
    assert len(manager._memory_store) == 0

def test_quarantine_data_returns_model():
    engine = SelfHealingEngine()

    item = engine.quarantine_data(
        {"pm25": 42},
        "processing failed",
    )

    assert isinstance(item, QuarantinedData)
    assert item.status == "pending"
    assert item.data == {"pm25": 42}
    assert item.quarantine_id

def test_replay_success_marks_record_replayed():
    manager = QuarantineManager()
    engine = SelfHealingEngine(quarantine_manager=manager)
    engine.quarantine_data({"pm25": 42}, "temporary failure")

    processed = []
    result = engine.replay_quarantined(processed.append)

    assert result == {"processed": 1, "succeeded": 1, "failed": 0}
    assert processed == [{"pm25": 42}]
    assert next(iter(manager._memory_store.values()))["status"] == "replayed"

def test_replay_failure_keeps_record_pending():
    manager = QuarantineManager()
    engine = SelfHealingEngine(quarantine_manager=manager)
    engine.quarantine_data({"pm25": 42}, "temporary failure")

    def failing_processor(data):
        raise RuntimeError("still unavailable")

    result = engine.replay_quarantined(failing_processor)

    assert result == {"processed": 1, "succeeded": 0, "failed": 1}
    record = next(iter(manager._memory_store.values()))
    assert record["status"] == "pending"
    assert record["replay_attempts"] == 1
    assert record["last_replay_error"] == "still unavailable"

def test_replay_only_processes_pending_records():
    manager = QuarantineManager()
    engine = SelfHealingEngine(quarantine_manager=manager)
    engine.quarantine_data({"id": 1}, "failure")

    engine.replay_quarantined(lambda data: None)
    processed = []

    result = engine.replay_quarantined(processed.append)

    assert result["processed"] == 0
    assert processed == []

def test_cleanup_removes_expired_records():
    manager = QuarantineManager(retention_days=1)
    record = manager.add_to_quarantine({"pm25": 42}, "failure")

    manager._memory_store[record["quarantine_id"]]["expires_at"] = (
        record["created_at"] - __import__("datetime").timedelta(seconds=1)
    )

    assert manager.cleanup() == 1
    assert len(manager._memory_store) == 0

def test_invalid_retry_configuration():
    with pytest.raises(ValueError):
        SelfHealingEngine(max_retries=-1)

def test_invalid_quarantine_limit():
    manager = QuarantineManager()

    with pytest.raises(ValueError):
        manager.replay(lambda data: None, limit=0)

def test_invalid_retention_period():
    with pytest.raises(ValueError):
        QuarantineManager(retention_days=0)
'@ | Set-Content -Encoding utf8 ..\tests\test_self_healing.py
```

## 4. Chạy test

Vẫn ở thư mục `src`, chạy:

powershell
```
$env:PYTHONPATH = (Get-Location).Path
python -m pytest ..\tests\test_self_healing.py -v
```

Nếu báo `No module named pytest`, cài pytest:

powershell
```
python -m pip install pytest
```

Sau đó chạy lại lệnh test.

Kết quả mong muốn là 12 tests passed.

## 5. Kiểm tra các file vừa tạo

Quay về thư mục gốc:

powershell
```
cd ..
git status
```

Nếu `.git` tồn tại và các file mới xuất hiện trong `git status`, kiểm tra tiếp:

powershell
```
git branch --show-current
git diff --check
```

Lưu ý: `git diff --check` không kiểm tra các file mới chưa được stage. Để kiểm tra cả file mới, có thể stage rồi chạy lại:

powershell
```
git add src/self_healing/engine.py src/self_healing/quarantine.py tests/test_self_healing.py
git diff --cached --check
git diff --cached --stat
```

Chỉ commit sau khi test chạy thành công và đã xem lại nội dung staged.

## 6. Commit Ngày 2

Nếu mọi thứ đều ổn:

powershell
```
git commit -m "feat: implement self-healing and quarantine"
git status
```

## Checklist hoàn thành

- [x]Tạo `engine.py` với `Incident`, `QuarantinedData`, `SelfHealingEngine`.
- [x]Tạo `quarantine.py` với `QuarantineManager`.
- [x]Viết unit tests cho retry, quarantine, replay và cleanup.
- [ ]Chạy test thành công.
- [ ]Kiểm tra thay đổi Git.
- [ ]Commit lên branch `data-pipeline`.

Quan trọng: Tui đã cung cấp code và lệnh để bà tạo file, nhưng chưa chạy trực tiếp trên máy bà nên chưa thể xác nhận tests thực sự pass hoặc commit đã thành công.

Bà chạy lệnh pytest trước, gửi tui kết quả nếu có lỗi. Mình sẽ sửa cho đến khi test chạy ổn rồi mới chốt Ngày 2 nhé.