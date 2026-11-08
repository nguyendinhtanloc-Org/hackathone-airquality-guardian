"""Apply Datadog monitor templates safely and report the result."""

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
DEFAULT_CONFIG = PROJECT_ROOT / "config" / "datadog" / "monitors.json"


def load_monitors(path: Path) -> list[dict[str, Any]]:
    """Load and validate monitor templates."""
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"Cannot read monitor configuration: {exc}") from exc

    if not isinstance(data, list):
        raise ValueError("Monitor configuration must be a JSON array.")

    required = {"name", "type", "query", "message", "options"}
    names: set[str] = set()

    for index, monitor in enumerate(data):
        if not isinstance(monitor, dict):
            raise ValueError(f"Monitor at index {index} must be an object.")

        missing = required - monitor.keys()
        if missing:
            raise ValueError(
                f"Monitor at index {index} is missing: {', '.join(sorted(missing))}"
            )

        name = monitor["name"]
        if not isinstance(name, str) or not name.strip():
            raise ValueError(f"Monitor at index {index} has an invalid name.")

        if name in names:
            raise ValueError(f"Duplicate monitor name in config: {name}")

        if not all(isinstance(monitor[field], str) and monitor[field].strip()
                   for field in ("type", "query", "message")):
            raise ValueError(f"Monitor {name!r} has an invalid type, query or message.")

        if not isinstance(monitor["options"], dict):
            raise ValueError(f"Monitor {name!r}: options must be an object.")

        names.add(name)

    return data


def apply_monitors(
    integrator: Any,
    monitors: list[dict[str, Any]],
    *,
    dry_run: bool = False,
    update_existing: bool = False,
) -> dict[str, Any]:
    """Apply monitor templates; abort on API errors rather than risk duplicates."""
    report: dict[str, Any] = {
        "started_at": datetime.now(timezone.utc).isoformat(),
        "status": "success",
        "dry_run": dry_run,
        "results": [],
    }

    try:
        response = integrator.client.get(
            "/api/v1/monitor",
            headers=integrator._headers(require_app_key=True),
        )
        response.raise_for_status()
        existing = response.json()
        if not isinstance(existing, list):
            raise RuntimeError("Datadog returned an unexpected monitor list.")
    except Exception as exc:
        report["status"] = "failed"
        report["error"] = f"Unable to list existing monitors: {type(exc).__name__}: {exc}"
        report["finished_at"] = datetime.now(timezone.utc).isoformat()
        return report

    by_name = {
        item.get("name"): item
        for item in existing
        if isinstance(item, dict) and isinstance(item.get("name"), str)
    }

    for template in monitors:
        name = template["name"]
        current = by_name.get(name)

        if current is not None:
            monitor_id = current.get("id")
            if not update_existing:
                report["results"].append({
                    "name": name,
                    "action": "skipped",
                    "status": "already_exists",
                    "monitor_id": monitor_id,
                })
                continue

            if not isinstance(monitor_id, int):
                report["results"].append({
                    "name": name,
                    "action": "update",
                    "status": "failed",
                    "error": "Existing monitor has no valid integer ID.",
                })
                report["status"] = "failed"
                break

            if dry_run:
                report["results"].append({
                    "name": name,
                    "action": "update",
                    "status": "planned",
                    "monitor_id": monitor_id,
                })
                continue

            try:
                response = integrator.client.put(
                    f"/api/v1/monitor/{monitor_id}",
                    headers=integrator._headers(require_app_key=True),
                    json=dict(template),
                )
                response.raise_for_status()
                report["results"].append({
                    "name": name,
                    "action": "update",
                    "status": "success",
                    "monitor_id": monitor_id,
                })
            except Exception as exc:
                report["results"].append({
                    "name": name,
                    "action": "update",
                    "status": "failed",
                    "error": f"{type(exc).__name__}: {exc}",
                })
                report["status"] = "failed"
                break
            continue

        if dry_run:
            report["results"].append({
                "name": name,
                "action": "create",
                "status": "planned",
            })
            continue

        try:
            created = integrator.create_monitor(template)
            report["results"].append({
                "name": name,
                "action": "create",
                "status": "success",
                "monitor_id": created.get("id"),
            })
            # Also protect against repeated names within one execution.
            by_name[name] = created
        except Exception as exc:
            report["results"].append({
                "name": name,
                "action": "create",
                "status": "failed",
                "error": f"{type(exc).__name__}: {exc}",
            })
            report["status"] = "failed"
            break

    report["finished_at"] = datetime.now(timezone.utc).isoformat()
    report["summary"] = {
        "total": len(report["results"]),
        "created": sum(r["action"] == "create" and r["status"] == "success"
                       for r in report["results"]),
        "updated": sum(r["action"] == "update" and r["status"] == "success"
                       for r in report["results"]),
        "skipped": sum(r["status"] == "already_exists"
                       for r in report["results"]),
        "planned": sum(r["status"] == "planned" for r in report["results"]),
        "failed": sum(r["status"] == "failed" for r in report["results"]),
    }
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Apply AirQuality Guardian Datadog monitor templates."
    )
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and plan changes without creating or updating monitors.",
    )
    parser.add_argument(
        "--update-existing", action="store_true",
        help="Update monitors with matching names to match the configuration.",
    )
    parser.add_argument(
        "--report", type=Path, metavar="PATH",
        help="Write the execution report as JSON.",
    )
    args = parser.parse_args(argv)

    try:
        monitors = load_monitors(args.config)
    except ValueError as exc:
        print(f"Configuration error: {exc}", file=sys.stderr)
        return 2

    integrator = None
    try:
        # Validate credentials before performing any API operation.
        from src.datadog.integrator import DatadogIntegrator

        integrator = DatadogIntegrator()
        integrator._headers(require_app_key=True)
        report = apply_monitors(
            integrator,
            monitors,
            dry_run=args.dry_run,
            update_existing=args.update_existing,
        )
    except (ValueError, RuntimeError) as exc:
        report = {
            "status": "failed",
            "error": f"{type(exc).__name__}: {exc}",
            "results": [],
        }
    finally:
        if integrator is not None:
            integrator.close()

    output = json.dumps(report, indent=2, ensure_ascii=False)
    print(output)

    if args.report:
        try:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(output + "\n", encoding="utf-8")
        except OSError as exc:
            print(f"Cannot write report: {exc}", file=sys.stderr)
            return 1

    return 0 if report["status"] == "success" else 1


if __name__ == "__main__":
    raise SystemExit(main())
