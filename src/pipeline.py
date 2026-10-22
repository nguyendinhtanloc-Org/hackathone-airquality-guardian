"""Orchestrate one fetch, validation, and MongoDB persistence cycle."""

from dataclasses import dataclass
from datetime import datetime, timezone
from time import perf_counter
from typing import Any
from uuid import uuid4

from src.aqi.calculator import AQICalculator
from src.database.mongodb import MongoDBClient
from src.fetcher.fetcher import DataFetcher
from src.self_healing.engine import Incident, SelfHealingEngine
from src.validation.validator import DataValidator


@dataclass(frozen=True)
class PipelineRunResult:
    """Summary and stored documents from one pipeline execution."""

    location_id: int
    records_fetched: int
    records_valid: int
    records_inserted: int
    records_failed: int
    duration_seconds: float
    documents: tuple[dict[str, Any], ...]
    incident: Incident | None = None
    status: str = "running"


class Pipeline:
    """Run the data ingestion flow through MongoDB."""

    def __init__(
        self,
        fetcher: DataFetcher,
        validator: DataValidator,
        database: MongoDBClient,
        aqi_calculator: AQICalculator | None = None,
        self_healing_engine: SelfHealingEngine | None = None,
    ) -> None:
        self.fetcher = fetcher
        self.validator = validator
        self.database = database
        self.aqi_calculator = aqi_calculator or AQICalculator()
        self.self_healing_engine = self_healing_engine or SelfHealingEngine()

    async def run_once(
        self,
        params: dict[str, Any] | None = None,
    ) -> PipelineRunResult:
        """Fetch, validate, persist, and summarize one execution.

        A failing fetch is reported through the returned result instead of
        raising, so the caller can record the incident and degrade
        gracefully.
        """
        started_at = perf_counter()
        request_params = params or {}
        location_id = request_params.get("location_id", 4946813)

        try:
            response = await self.fetcher.fetch(request_params)
        except Exception as error:
            incident = self.self_healing_engine.handle_error(
                error,
                data=request_params,
                component="fetcher",
            )
            return PipelineRunResult(
                location_id=location_id,
                records_fetched=0,
                records_valid=0,
                records_inserted=0,
                records_failed=0,
                duration_seconds=perf_counter() - started_at,
                documents=(),
                incident=incident,
                status="degraded",
            )

        fetched_count = len(response.get("results", []))
        measurements = self.validator.validate(response)
        inserted_documents: list[dict[str, Any]] = []
        for measurement in measurements:
            timestamp = self._parse_timestamp(measurement.last_updated)
            aqi_value = self.aqi_calculator.calculate(
                measurement.parameter,
                measurement.value,
            )
            aqi_data: dict[str, Any] = {}
            if aqi_value is not None:
                level = self.aqi_calculator.get_level(aqi_value)
                aqi_data = {
                    f"{measurement.parameter}_aqi": aqi_value,
                    "primary_pollutant": measurement.parameter,
                    **level,
                }

            document = {
                "event_id": str(uuid4()),
                "metadata": {"location_id": location_id},
                "timestamp": timestamp,
                "measurements": {
                    measurement.parameter: measurement.value,
                    f"{measurement.parameter}_unit": measurement.unit,
                },
                "aqi": aqi_data,
            }
            self.database.insert_air_quality(document)
            inserted_documents.append(document)

        return PipelineRunResult(
            location_id=location_id,
            records_fetched=fetched_count,
            records_valid=len(measurements),
            records_inserted=len(inserted_documents),
            records_failed=max(fetched_count - len(measurements), 0),
            duration_seconds=perf_counter() - started_at,
            documents=tuple(inserted_documents),
        )

    @staticmethod
    def _parse_timestamp(value: str | None) -> datetime:
        if not value:
            raise ValueError("A measurement timestamp is required for persistence.")
        try:
            timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as error:
            raise ValueError(
                f"Invalid measurement timestamp: {value!r}."
            ) from error
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        return timestamp.astimezone(timezone.utc)
