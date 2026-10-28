import json
from pathlib import Path
from unittest.mock import MagicMock

from scripts.apply_datadog import apply_monitors, load_monitors


def monitor(name="AirQuality Guardian: pipeline degraded"):
    return {
        "name": name,
        "type": "query alert",
        "query": "sum(last_5m):sum:pipeline.records.failed{*} > 0",
        "message": "Check pipeline.",
        "options": {"thresholds": {"critical": 0}},
    }


def integrator(existing=None):
    client = MagicMock()
    response = MagicMock()
    response.json.return_value = existing or []
    client.get.return_value = response

    instance = MagicMock()
    instance.client = client
    instance._headers.return_value = {
        "DD-API-KEY": "test",
        "DD-APPLICATION-KEY": "test",
    }
    instance.create_monitor.side_effect = lambda payload: {
        "id": 123,
        "name": payload["name"],
    }
    return instance


def test_load_monitors_reads_project_configuration():
    path = Path(__file__).resolve().parents[2] / "config" / "datadog" / "monitors.json"
    monitors = load_monitors(path)
    assert len(monitors) == 3
    assert all(item["name"] for item in monitors)


def test_load_monitors_rejects_duplicate_names(tmp_path):
    path = tmp_path / "monitors.json"
    path.write_text(json.dumps([monitor(), monitor()]), encoding="utf-8")

    try:
        load_monitors(path)
        assert False, "Expected duplicate names to be rejected"
    except ValueError as exc:
        assert "Duplicate monitor name" in str(exc)


def test_creates_missing_monitor():
    api = integrator()
    result = apply_monitors(api, [monitor()])

    assert result["status"] == "success"
    assert result["summary"]["created"] == 1
    api.create_monitor.assert_called_once()


def test_existing_monitor_is_not_duplicated():
    api = integrator([{"id": 123, "name": monitor()["name"]}])
    result = apply_monitors(api, [monitor()])

    assert result["status"] == "success"
    assert result["summary"]["skipped"] == 1
    api.create_monitor.assert_not_called()


def test_dry_run_does_not_create_or_update():
    api = integrator()
    result = apply_monitors(api, [monitor()], dry_run=True)

    assert result["results"][0]["status"] == "planned"
    assert result["summary"]["planned"] == 1
    api.create_monitor.assert_not_called()
    api.client.put.assert_not_called()


def test_update_existing_monitor():
    api = integrator([{"id": 123, "name": monitor()["name"]}])
    api.client.put.return_value = MagicMock()

    result = apply_monitors(api, [monitor()], update_existing=True)

    assert result["summary"]["updated"] == 1
    api.client.put.assert_called_once()
    assert api.client.put.call_args.args[0] == "/api/v1/monitor/123"


def test_list_failure_aborts_without_creating():
    api = integrator()
    api.client.get.return_value.raise_for_status.side_effect = RuntimeError(
        "Datadog unavailable"
    )

    result = apply_monitors(api, [monitor()])

    assert result["status"] == "failed"
    api.create_monitor.assert_not_called()


def test_create_failure_is_reported():
    api = integrator()
    api.create_monitor.side_effect = RuntimeError("API unavailable")

    result = apply_monitors(api, [monitor()])

    assert result["status"] == "failed"
    assert result["summary"]["failed"] == 1
    assert result["results"][0]["status"] == "failed"
