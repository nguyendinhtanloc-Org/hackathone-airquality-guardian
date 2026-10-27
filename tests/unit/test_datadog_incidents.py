"""Unit tests for incident reporting to Datadog (stage 2.5)."""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src import main as pipeline_main


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("src.pipeline.asyncio.sleep", new=_instant_sleep()):
        yield


def _instant_sleep():
    async def sleep(_seconds):
        return None

    return sleep


def make_result(status="running", incident=None):
    result = MagicMock()
    result.status = status
    result.location_id = 4946813
    result.records_fetched = 3
    result.records_valid = 3
    result.records_inserted = 1 if status == "running" else 0
    result.records_failed = 0
    result.duration_seconds = 0.2
    result.documents = (
        (
            {
                "metadata": {"location_id": 4946813},
                "timestamp": datetime.now(timezone.utc),
                "measurements": {"pm25": 12.5},
                "aqi": {"pm25_aqi": 52},
            },
        )
        if status == "running"
        else ()
    )
    result.incident = incident
    return result


def make_incident():
    incident = MagicMock()
    incident.component = "fetcher"
    incident.error = "OpenAQ request timed out"
    incident.retryable = True
    incident.to_dict.return_value = {
        "error": "OpenAQ request timed out",
        "error_type": "TimeoutError",
        "component": "fetcher",
        "retryable": True,
        "attempt": 3,
        "max_retries": 3,
        "incident_id": "abc-123",
        "created_at": datetime(2026, 10, 25, 10, 0, tzinfo=timezone.utc),
        "metadata": {"component": "fetcher"},
    }
    return incident


def build(database, pipeline, datadog=None):
    """Patches so that run_cycle's inputs are controlled objects."""
    return {
        "database": database,
        "datadog": datadog,
        "pipeline": pipeline,
    }


@pytest.mark.asyncio
async def test_cycle_without_incident_sends_info_log_and_zero_metric():
    database = MagicMock()
    datadog = MagicMock()
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock(return_value=make_result(status="running"))
    pipeline.self_healing_engine.incidents = []

    await pipeline_main.run_cycle(
        database, datadog, pipeline, 4946813
    )

    datadog.send_metrics.assert_called_once()
    sent_metrics = datadog.send_metrics.call_args.args[0]
    assert sent_metrics["pipeline.incidents.count"] == 0
    assert sent_metrics["datavalue.aqi.current"] == 52

    log_call = datadog.send_logs.call_args
    assert log_call.kwargs["status"] == "info"
    database.update_pipeline_status.assert_called_once()
    # First positional arg is the status, and it carries 0 incidents.
    assert database.update_pipeline_status.call_args.args[0] == "running"
    assert database.update_pipeline_status.call_args.kwargs["incidents"] == []


@pytest.mark.asyncio
async def test_cycle_with_incident_sends_error_logs():
    database = MagicMock()
    datadog = MagicMock()
    incident = make_incident()
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock(
        return_value=make_result(status="degraded", incident=incident)
    )
    pipeline.self_healing_engine.incidents = [incident]

    await pipeline_main.run_cycle(database, datadog, pipeline, 4946813)

    # 1 status log + 1 log per incident.
    assert datadog.send_logs.call_count == 2

    # The run log is escalated to status=error when incidents exist.
    run_log = datadog.send_logs.call_args_list[0]
    assert run_log.kwargs["status"] == "error"
    assert run_log.args[0]["incidents"] == 1

    # The incident log carries the incident id for correlation.
    incident_log = datadog.send_logs.call_args_list[1]
    assert incident_log.kwargs["status"] == "error"
    assert incident_log.args[0]["incident_id"] == "abc-123"
    assert incident_log.args[0]["created_at"] == "2026-10-25T10:00:00+00:00"

    # The metric counts the incidents of this cycle only.
    sent_metrics = datadog.send_metrics.call_args.args[0]
    assert sent_metrics["pipeline.incidents.count"] == 1

    # The same incident is mirrored into the pipeline_status record.
    status_call = database.update_pipeline_status.call_args
    assert status_call.args[0] == "degraded"
    assert status_call.kwargs["incidents"] == [incident.to_dict.return_value]


@pytest.mark.asyncio
async def test_datadog_failure_does_not_break_the_cycle():
    database = MagicMock()
    datadog = MagicMock()
    datadog.send_metrics.side_effect = RuntimeError("intake down")
    datadog.send_logs.side_effect = RuntimeError("intake down")
    incident = make_incident()
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock(
        return_value=make_result(status="degraded", incident=incident)
    )
    pipeline.self_healing_engine.incidents = [incident]

    # Must not raise: observability outages must not kill the pipeline.
    await pipeline_main.run_cycle(database, datadog, pipeline, 4946813)

    database.update_pipeline_status.assert_called_once()


@pytest.mark.asyncio
async def test_no_datadog_is_a_noop_for_observability():
    database = MagicMock()
    incident = make_incident()
    pipeline = MagicMock()
    pipeline.run_once = AsyncMock(
        return_value=make_result(status="degraded", incident=incident)
    )
    pipeline.self_healing_engine.incidents = [incident]

    await pipeline_main.run_cycle(database, None, pipeline, 4946813)

    database.update_pipeline_status.assert_called_once()
