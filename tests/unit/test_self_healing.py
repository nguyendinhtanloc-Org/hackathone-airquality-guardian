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
