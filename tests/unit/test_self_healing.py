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
def test_replay_failure_caps_out_to_failed_status():
    manager = QuarantineManager(max_replay_attempts=2)
    engine = SelfHealingEngine(quarantine_manager=manager)
    engine.quarantine_data({"pm25": 42}, "temporary failure")

    def failing(_data):
        raise RuntimeError("still down")

    # First attempt: attempts=1 < cap, stays pending for a later replay.
    first = engine.replay_quarantined(failing)
    record = next(iter(manager._memory_store.values()))
    assert first == {"processed": 1, "succeeded": 0, "failed": 1}
    assert record["replay_attempts"] == 1
    assert record["status"] == "pending"

    # Second attempt reaches the cap: record is parked as "failed".
    second = engine.replay_quarantined(failing)
    assert second == {"processed": 1, "succeeded": 0, "failed": 1}
    assert record["status"] == "failed"
    assert record["replay_attempts"] == 2

    # A failed record is no longer eligible for replay.
    third = engine.replay_quarantined(failing)
    assert third == {"processed": 0, "succeeded": 0, "failed": 0}


def test_replay_cap_requires_positive_limit():
    with pytest.raises(ValueError):
        QuarantineManager(max_replay_attempts=0)


# --- concurrency: CAS claim guard (two workers, one record) ------------


def test_two_workers_cannot_claim_the_same_record():
    manager_a = QuarantineManager()
    record = manager_a.add_to_quarantine({"location_id": 4946813}, "boom")

    # Worker B shares the same store (simulates two processes that both
    # talk to the same MongoDB collection).
    manager_b = QuarantineManager()
    manager_b._memory_store = manager_a._memory_store

    # Worker A claims the record first: pending -> in_flight (CAS guard
    # also bumps replay_attempts to 1 on claim).
    claimed_a = manager_a._pending_for_replay(limit=10)
    assert [rid for rid, _ in claimed_a] == [record["quarantine_id"]]
    assert manager_a._memory_store[record["quarantine_id"]]["status"] == "in_flight"
    assert manager_a._memory_store[record["quarantine_id"]]["replay_attempts"] == 1

    # Fresh in_flight claims are NOT pending and NOT lease-expired, so
    # Worker B's read finds nothing: no double claim, no double attempts.
    claimed_b = manager_b._pending_for_replay(limit=10)
    assert claimed_b == []
    assert manager_a._memory_store[record["quarantine_id"]]["replay_attempts"] == 1

    # Settling is CAS-guarded too: only a claim from in_flight applies.
    # (A failed record would still be in_flight here, so this path is
    # exercised via test_failed_record_cannot_be_flipped_back_to_replayed.)
    assert manager_a._mark_replayed(record["quarantine_id"]) is True
    assert manager_a._memory_store[record["quarantine_id"]]["status"] == "replayed"


def test_failed_record_cannot_be_flipped_back_to_replayed():
    manager = QuarantineManager(max_replay_attempts=1)
    record = manager.add_to_quarantine({"location_id": 4946813}, "boom")

    def failing(_data):
        raise RuntimeError("still down")

    manager.replay(failing)
    assert manager._memory_store[record["quarantine_id"]]["status"] == "failed"

    # A late "success" from another worker must not resurrect a parked record.
    assert manager._mark_replayed(record["quarantine_id"]) is False
    assert manager._memory_store[record["quarantine_id"]]["status"] == "failed"


def test_stale_in_flight_claim_is_reclaimed_after_lease():
    from datetime import timedelta

    manager = QuarantineManager(claim_lease_seconds=10)
    record = manager.add_to_quarantine({"location_id": 4946813}, "boom")
    rid = record["quarantine_id"]

    # A worker crashed mid-replay: claim is in_flight, fresh.
    manager._update_record(rid, status="in_flight", in_flight_at=_utcnow())
    assert manager._pending_for_replay(limit=10) == []

    # The same claim older than the lease is reclaimed (retried).
    stale = _utcnow() - timedelta(seconds=60)
    manager._memory_store[rid]["in_flight_at"] = stale
    claimed = manager._pending_for_replay(limit=10)
    assert [rid_ for rid_, _ in claimed] == [rid]


def _utcnow():
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


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
