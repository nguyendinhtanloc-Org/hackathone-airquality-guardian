"""Datadog metrics, logs, and monitor API integration."""

import math
import os
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

import httpx
from dotenv import load_dotenv


class DatadogIntegrator:
    """Send pipeline telemetry to a Datadog site."""

    def __init__(
        self,
        api_key: str | None = None,
        app_key: str | None = None,
        site: str | None = None,
        client: httpx.Client | None = None,
    ) -> None:
        if api_key is None or app_key is None or site is None:
            load_dotenv()

        self.api_key = (
            api_key if api_key is not None else os.getenv("DATADOG_API_KEY")
        )
        self.app_key = (
            app_key if app_key is not None else os.getenv("DATADOG_APP_KEY")
        )
        configured_site = site if site is not None else os.getenv("DATADOG_SITE")
        self.site = (configured_site or "datadoghq.com").strip()
        if not self.site or "/" in self.site:
            raise ValueError("DATADOG_SITE must be a Datadog site domain.")

        self.client = (
            client
            if client is not None
            else httpx.Client(
                base_url=f"https://api.{self.site}",
                timeout=10.0,
            )
        )

    def _headers(self, *, require_app_key: bool = False) -> dict[str, str]:
        if not self.api_key:
            raise ValueError("DATADOG_API_KEY must be configured.")
        headers = {"DD-API-KEY": self.api_key}
        if require_app_key:
            if not self.app_key:
                raise ValueError("DATADOG_APP_KEY must be configured.")
            headers["DD-APPLICATION-KEY"] = self.app_key
        return headers

    def send_metrics(
        self,
        metrics: Mapping[str, int | float],
        tags: Sequence[str] | None = None,
        timestamp: datetime | None = None,
    ) -> dict[str, Any]:
        """Submit numeric gauge metrics to the Datadog v2 intake API."""
        if not metrics:
            raise ValueError("At least one metric is required.")
        if any(
            not name
            or isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            for name, value in metrics.items()
        ):
            raise ValueError("Metric names and values must be valid finite numbers.")

        point_time = timestamp or datetime.now(timezone.utc)
        series = [
            {
                "metric": name,
                "type": 3,
                "points": [[point_time.timestamp(), value]],
                "tags": list(tags or []),
            }
            for name, value in metrics.items()
        ]
        response = self.client.post(
            "/api/v2/series",
            headers=self._headers(),
            json={"series": series},
        )
        response.raise_for_status()
        return response.json()

    def send_logs(
        self,
        log: str | Mapping[str, Any],
        status: str = "info",
        service: str = "airquality-pipeline",
        source: str = "python",
    ) -> None:
        """Submit one structured log to the Datadog v2 logs intake API."""
        if not service or not source or not status:
            raise ValueError("Log status, service, and source must not be empty.")
        attributes = dict(log) if isinstance(log, Mapping) else {"message": log}
        if not attributes.get("message"):
            raise ValueError("A log message is required.")
        attributes.update(
            {
                "ddsource": source,
                "service": service,
                "status": status,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
        response = self.client.post(
            "/api/v2/logs",
            headers=self._headers(),
            json=[attributes],
        )
        response.raise_for_status()

    def create_monitor(self, monitor: Mapping[str, Any]) -> dict[str, Any]:
        """Create one monitor from a Datadog monitor API request template."""
        required_fields = {"name", "type", "query", "message", "options"}
        missing = required_fields.difference(monitor)
        if missing:
            raise ValueError(
                f"Monitor is missing required fields: {', '.join(sorted(missing))}."
            )
        response = self.client.post(
            "/api/v1/monitor",
            headers=self._headers(require_app_key=True),
            json=dict(monitor),
        )
        response.raise_for_status()
        return response.json()

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self.client.close()
