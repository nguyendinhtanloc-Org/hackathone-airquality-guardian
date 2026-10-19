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