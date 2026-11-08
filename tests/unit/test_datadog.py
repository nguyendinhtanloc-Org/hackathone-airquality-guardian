from datetime import datetime, timezone
import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from src.datadog.integrator import DatadogIntegrator


@pytest.fixture
def datadog():
    http_client = MagicMock()
    http_client.post.return_value.json.return_value = {"status": "ok"}
    return DatadogIntegrator(
        api_key="test-api-key",
        app_key="test-app-key",
        site="datadoghq.com",
        client=http_client,
    ), http_client


def test_send_metrics_posts_datadog_v2_series(datadog):
    integrator, http_client = datadog
    timestamp = datetime(2026, 10, 19, tzinfo=timezone.utc)

    result = integrator.send_metrics(
        {"pipeline.records.valid": 12},
        tags=["env:test"],
        timestamp=timestamp,
    )

    assert result == {"status": "ok"}
    args, kwargs = http_client.post.call_args
    assert args == ("/api/v2/series",)
    assert kwargs["headers"] == {"DD-API-KEY": "test-api-key"}
    assert kwargs["json"]["series"] == [
        {
            "metric": "pipeline.records.valid",
            "type": 3,
            "points": [
                {
                    "timestamp": int(timestamp.timestamp()),
                    "value": 12,
                }
            ],
            "tags": ["env:test"],
        }
    ]


@pytest.mark.parametrize(
    "metrics",
    [{}, {"": 1}, {"pipeline.invalid": float("nan")}, {"pipeline.invalid": True}],
)
def test_send_metrics_rejects_invalid_input(datadog, metrics):
    integrator, http_client = datadog

    with pytest.raises(ValueError):
        integrator.send_metrics(metrics)

    http_client.post.assert_not_called()


def test_send_logs_posts_structured_log(datadog):
    integrator, http_client = datadog

    integrator.send_logs(
        {"message": "Fetched measurements", "records_count": 3},
        status="info",
    )

    args, kwargs = http_client.post.call_args
    assert args == ("https://http-intake.logs.datadoghq.com/api/v2/logs",)
    assert kwargs["headers"] == {"DD-API-KEY": "test-api-key"}
    log = kwargs["json"][0]
    assert log["message"] == "Fetched measurements"
    assert log["records_count"] == 3
    assert log["ddsource"] == "python"
    assert log["service"] == "airquality-pipeline"
    assert log["status"] == "info"
    assert log["timestamp"]


def test_send_logs_requires_message(datadog):
    integrator, http_client = datadog

    with pytest.raises(ValueError, match="log message"):
        integrator.send_logs({"records_count": 3})

    http_client.post.assert_not_called()


def test_create_monitor_posts_with_api_and_application_keys(datadog):
    integrator, http_client = datadog
    monitor = {
        "name": "Pipeline degraded",
        "type": "query alert",
        "query": "sum(last_5m):sum:pipeline.records.failed{*} > 0",
        "message": "Check the pipeline.",
        "options": {"thresholds": {"critical": 0}},
    }

    result = integrator.create_monitor(monitor)

    assert result == {"status": "ok"}
    args, kwargs = http_client.post.call_args
    assert args == ("/api/v1/monitor",)
    assert kwargs["headers"] == {
        "DD-API-KEY": "test-api-key",
        "DD-APPLICATION-KEY": "test-app-key",
    }
    assert kwargs["json"] == monitor


def test_create_monitor_requires_application_key():
    http_client = MagicMock()
    integrator = DatadogIntegrator(
        api_key="test-api-key",
        app_key="",
        client=http_client,
    )
    monitor = {
        "name": "Pipeline degraded",
        "type": "query alert",
        "query": "sum(last_5m):sum:pipeline.records.failed{*} > 0",
        "message": "Check the pipeline.",
        "options": {},
    }

    with pytest.raises(ValueError, match="DATADOG_APP_KEY"):
        integrator.create_monitor(monitor)

    http_client.post.assert_not_called()


def test_create_monitor_rejects_incomplete_payload(datadog):
    integrator, http_client = datadog

    with pytest.raises(ValueError, match="missing required fields"):
        integrator.create_monitor({"name": "Incomplete"})

    http_client.post.assert_not_called()


def _list_response(http_client, monitors):
    http_client.get.return_value.json.return_value = monitors
    http_client.get.return_value.raise_for_status.return_value = None


def test_ensure_monitor_creates_when_missing(datadog):
    integrator, http_client = datadog
    _list_response(http_client, [])
    http_client.post.return_value.json.return_value = {"id": 99}
    template = {
        "name": "Pipeline degraded",
        "type": "query alert",
        "query": "sum(last_5m):sum:pipeline.records.failed{*} > 0",
        "message": "Check the pipeline.",
        "options": {"thresholds": {"critical": 0}},
    }

    assert integrator.ensure_monitor("Pipeline degraded", template) == 99
    http_client.post.assert_called_once()


def test_ensure_monitor_is_noop_when_it_already_exists(datadog):
    integrator, http_client = datadog
    _list_response(http_client, [{"id": 7, "name": "Pipeline degraded"}])

    assert integrator.ensure_monitor("Pipeline degraded", {}) == 7
    http_client.post.assert_not_called()


def test_ensure_monitor_looks_up_by_name(datadog):
    integrator, http_client = datadog
    _list_response(http_client, [])
    http_client.post.return_value.json.return_value = {"id": 1}

    integrator.ensure_monitor(
        "AirQuality Guardian: pipeline degraded",
        {"name": "x", "type": "t", "query": "q", "message": "m", "options": {}},
    )

    get_args, get_kwargs = http_client.get.call_args
    assert get_args[0] == "/api/v1/monitor"
    assert get_kwargs["params"]["name"] == "AirQuality Guardian: pipeline degraded"
    assert "DD-APPLICATION-KEY" in get_kwargs["headers"]


def test_api_failures_are_not_silenced(datadog):
    integrator, http_client = datadog
    http_client.post.return_value.raise_for_status.side_effect = RuntimeError(
        "Datadog API unavailable"
    )

    with pytest.raises(RuntimeError, match="Datadog API unavailable"):
        integrator.send_metrics({"pipeline.records.valid": 12})



def test_dashboard_and_monitor_templates_are_valid_api_payloads():
    project_root = Path(__file__).resolve().parents[2]

    dashboard = json.loads(
        (project_root / "config" / "datadog" / "dashboard.json").read_text(
            encoding="utf-8"
        )
    )
    monitors = json.loads(
        (project_root / "config" / "datadog" / "monitors.json").read_text(
            encoding="utf-8"
        )
    )

    assert dashboard["layout_type"] == "ordered"
    assert {v["name"] for v in dashboard["template_variables"]} == {
        "env", "location_id", "scenario",
    }
    # v2 layout: every top-level widget is a group of real widgets.
    assert all(w["definition"]["type"] == "group" for w in dashboard["widgets"])
    inner_types = {
        inner["definition"]["type"]
        for group in dashboard["widgets"]
        for inner in group["definition"]["widgets"]
    }
    assert {"timeseries", "query_value", "log_stream"} <= inner_types

    assert len(monitors) == 3
    assert all(
        {"name", "type", "query", "message", "options"} <= monitor.keys()
        for monitor in monitors
    )
