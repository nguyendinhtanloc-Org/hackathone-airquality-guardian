
"""Create the AirQuality Guardian dashboard in Datadog."""

import json
import os
import sys
from pathlib import Path

import httpx
from dotenv import load_dotenv


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD_FILE = ROOT / "config" / "datadog" / "dashboard.json"


def main() -> int:
    load_dotenv(ROOT / ".env")

    api_key = os.getenv("DATADOG_API_KEY")
    app_key = os.getenv("DATADOG_APP_KEY")
    site = (os.getenv("DATADOG_SITE") or "datadoghq.com").strip()

    if not api_key or not app_key:
        print("Missing DATADOG_API_KEY or DATADOG_APP_KEY in .env")
        return 1

    if not site or "/" in site:
        print("Invalid DATADOG_SITE.")
        return 1

    dashboard = json.loads(DASHBOARD_FILE.read_text(encoding="utf-8"))

    headers = {
        "DD-API-KEY": api_key,
        "DD-APPLICATION-KEY": app_key,
        "Content-Type": "application/json",
    }

    # Check for an existing dashboard with the same title to avoid duplicates.
    with httpx.Client(
        base_url=f"https://api.{site}",
        headers=headers,
        timeout=20.0,
    ) as client:
        response = client.get("/api/v1/dashboard")
        if response.is_error:
            print("Datadog HTTP status:", response.status_code)
            print("Datadog response:", response.text)
            return 1

        existing = next(
            (
                item
                for item in response.json().get("dashboards", [])
                if item.get("title") == dashboard["title"]
            ),
            None,
        )

        if existing:
            dashboard_id = existing["id"]
            response = client.put(
                f"/api/v1/dashboard/{dashboard_id}",
                json={**dashboard, "id": dashboard_id},
            )
            action = "updated"
        else:
            response = client.post(
                "/api/v1/dashboard",
                json=dashboard,
            )
            action = "created"

        if response.is_error:
            print("Datadog HTTP status:", response.status_code)
            print("Datadog response:", response.text)
            return 1

        result = response.json()

    print(
        json.dumps(
            {
                "status": "success",
                "action": action,
                "title": result.get("title", dashboard["title"]),
                "dashboard_id": result.get("id"),
                "url": result.get("url"),
                "widgets": len(dashboard["widgets"]),
            },
            indent=2,
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (httpx.HTTPError, ValueError, OSError) as exc:
        print(f"Dashboard operation failed: {exc}", file=sys.stderr)
        raise SystemExit(1)