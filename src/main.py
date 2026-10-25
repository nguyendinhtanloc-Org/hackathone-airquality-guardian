"""
AirQuality Guardian - Main Entry Point
"""

import argparse
import asyncio
import os
import sys
from collections.abc import Sequence

import httpx
from dotenv import load_dotenv
from loguru import logger

from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator
from src.aqi.calculator import AQICalculator
from src.database.mongodb import MongoDBClient
from src.datadog.integrator import DatadogIntegrator
from src.pipeline import Pipeline
from src.self_healing.engine import SelfHealingEngine
from src.self_healing.quarantine import QuarantineManager

load_dotenv()

logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))

DEFAULT_LOCATION_ID = 4946813
DEFAULT_REPLAY_LIMIT = 100


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Parse the command line for the pipeline entry point.

    ``argv`` defaults to the real process arguments so that
    ``python -m src.main --replay`` works from a terminal.
    """
    parser = argparse.ArgumentParser(
        prog="airquality-guardian",
        description="AirQuality Guardian ingestion pipeline.",
    )
    parser.add_argument(
        "--replay",
        action="store_true",
        help="Replay quarantined records instead of fetching new data.",
    )
    parser.add_argument(
        "--replay-limit",
        type=int,
        default=DEFAULT_REPLAY_LIMIT,
        help="Maximum number of quarantined records to replay.",
    )
    parser.add_argument(
        "--location-id",
        type=int,
        default=DEFAULT_LOCATION_ID,
        help="Location id to fetch measurements for.",
    )
    return parser.parse_args(
        list(argv) if argv is not None else sys.argv[1:]
    )


def build_pipeline(
    database: MongoDBClient,
) -> tuple[Pipeline, SelfHealingEngine]:
    """Wire the fetcher, validator, database, and self-healing engine."""
    fetcher = DataFetcher()
    validator = DataValidator()
    self_healing_engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=database.quarantine_events,
        ),
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
    )
    pipeline = Pipeline(
        fetcher,
        validator,
        database,
        self_healing_engine=self_healing_engine,
    )
    return pipeline, self_healing_engine


async def replay_quarantined(
    pipeline: Pipeline,
    self_healing_engine: SelfHealingEngine,
    limit: int,
) -> dict[str, int]:
    """Replay quarantined records through the pipeline.

    A record only counts as replayed when the pipeline actually persists
    data again, so a still-failing fetch leaves the record pending.
    """

    async def processor(data: object) -> None:
        params = data if isinstance(data, dict) else {}
        result = await pipeline.run_once(params)
        if result.status != "running" or result.records_inserted == 0:
            raise RuntimeError(
                f"Replay did not persist data (status={result.status})."
            )

    summary = await self_healing_engine.replay_quarantined_async(
        processor,
        limit=limit,
    )
    logger.info(
        "Replay finished: processed={} succeeded={} failed={}",
        summary["processed"],
        summary["succeeded"],
        summary["failed"],
    )
    return summary


async def main(argv: Sequence[str] | None = None) -> None:
    args = parse_args(argv)
    logger.info("Starting AirQuality Guardian Pipeline")

    database = MongoDBClient()
    logger.info("MongoDB database: {}", database.database.name)
    logger.info("MongoDB collection: {}", database.air_quality.name)
    logger.info("MongoDB ping: {}", database.client.admin.command("ping"))
    datadog = (
        DatadogIntegrator()
        if os.getenv("DATADOG_API_KEY")
        else None
    )
    pipeline, self_healing_engine = build_pipeline(database)
    try:
        if args.replay:
            await replay_quarantined(
                pipeline,
                self_healing_engine,
                limit=args.replay_limit,
            )
            return
        result = await pipeline.run_once({"location_id": args.location_id})
        if result.incident is not None:
            logger.error(
                "Pipeline incident on component {}: {} (retryable={})",
                result.incident.component,
                result.incident.error,
                result.incident.retryable,
            )
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

        if datadog is None:
            logger.warning("DATADOG_API_KEY is not configured; skipping Datadog.")
        else:
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

        # Cập nhật pipeline status vào MongoDB (2.4)
        incidents = [
            inc.to_dict() for inc in pipeline.self_healing_engine.incidents
        ]
        database.update_pipeline_status(
            result.status,
            metrics=metrics,
            incidents=incidents,
        )
    finally:
        if datadog is not None:
            datadog.close()
        database.close()


if __name__ == "__main__":
    asyncio.run(main())