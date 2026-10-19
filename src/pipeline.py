"""Orchestrate one fetch, validation, and MongoDB persistence cycle."""

from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from src.aqi.calculator import AQICalculator
from src.database.mongodb import MongoDBClient
from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator


class Pipeline:
    """Run the data ingestion flow through MongoDB."""

    def __init__(
        self,
        fetcher: DataFetcher,
        validator: DataValidator,
        database: MongoDBClient,
        aqi_calculator: AQICalculator | None = None,
    ) -> None:
        self.fetcher = fetcher
        self.validator = validator
        self.database = database
        self.aqi_calculator = aqi_calculator or AQICalculator()

    async def run_once(self, params: dict[str, Any] | None = None) -> int:
        """Fetch, validate, and persist measurements; return the count saved."""
        request_params = params or {}
        response = await self.fetcher.fetch(request_params)
        if response is None:
            raise RuntimeError("Air-quality fetch failed; no data was persisted.")

        measurements = self.validator.validate(response)
        location_id = request_params.get("location_id", 4946813)
        inserted_count = 0
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

            self.database.insert_air_quality(
                {
                    "event_id": str(uuid4()),
                    "metadata": {"location_id": location_id},
                    "timestamp": timestamp,
                    "measurements": {
                        measurement.parameter: measurement.value,
                        f"{measurement.parameter}_unit": measurement.unit,
                    },
                    "aqi": aqi_data,
                }
            )
            inserted_count += 1
        return inserted_count

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
