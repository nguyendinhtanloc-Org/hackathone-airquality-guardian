"""
AirQuality Guardian - Main Entry Point
"""

import asyncio
import os

import httpx
from dotenv import load_dotenv
from loguru import logger

from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator
from src.aqi.calculator import AQICalculator
from src.database.mongodb import MongoDBClient
from src.datadog.integrator import DatadogIntegrator
from src.pipeline import Pipeline

load_dotenv()

logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))


async def main() -> None:
    logger.info("Starting AirQuality Guardian Pipeline")

    database = MongoDBClient()
    datadog = (
        DatadogIntegrator()
        if os.getenv("DATADOG_API_KEY")
        else None
    )
    fetcher = DataFetcher()
    validator = DataValidator()
    pipeline = Pipeline(fetcher, validator, database)
    try:
        result = await pipeline.run_once({"location_id": 4946813})
        logger.info(
            "Pipeline run complete: location_id={} fetched={} valid={} "
            "inserted={} failed={} duration_seconds={:.2f}",
            result.location_id,
            result.records_fetched,
            result.records_valid,
            result.records_inserted,
            result.records_failed,
            result.duration_seconds,
        )
        for document in result.documents:
            logger.info(
                "MongoDB document: location_id={} timestamp={} measurements={} aqi={}",
                document["metadata"]["location_id"],
                document["timestamp"].isoformat(),
                document["measurements"],
                document["aqi"],
            )

        if datadog is None:
            logger.warning("DATADOG_API_KEY is not configured; skipping Datadog.")
        else:
            metrics: dict[str, int | float] = {
                "pipeline.records.fetched": result.records_fetched,
                "pipeline.records.valid": result.records_valid,
                "pipeline.records.inserted": result.records_inserted,
                "pipeline.records.failed": result.records_failed,
                "pipeline.fetch.latency": result.duration_seconds,
            }
            aqis = [
                value
                for document in result.documents
                for key, value in document["aqi"].items()
                if key.endswith("_aqi") and isinstance(value, (int, float))
            ]
            if aqis:
                metrics["datavalue.aqi.current"] = max(aqis)
            try:
                metric_result = datadog.send_metrics(
                    metrics,
                    tags=[
                        "env:dev",
                        "service:airquality-pipeline",
                        f"location_id:{result.location_id}",
                    ],
                )
                logger.info("Datadog accepted metrics: {}", metric_result)
            except httpx.HTTPError as error:
                logger.exception("Datadog metric submission failed: {}", error)
            try:
                datadog.send_logs(
                    {
                        "message": "AirQuality pipeline run completed",
                        "location_id": result.location_id,
                        "records_fetched": result.records_fetched,
                        "records_valid": result.records_valid,
                        "records_inserted": result.records_inserted,
                        "records_failed": result.records_failed,
                        "duration_seconds": result.duration_seconds,
                    }
                )
                logger.info("Datadog accepted pipeline log.")
            except httpx.HTTPError as error:
                logger.exception("Datadog log submission failed: {}", error)
    finally:
        if datadog is not None:
            datadog.close()
        database.close()


if __name__ == "__main__":
    asyncio.run(main())