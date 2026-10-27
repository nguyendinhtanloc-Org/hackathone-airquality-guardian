"""
AirQuality Guardian - Main Entry Point
"""

import argparse
import asyncio
import os
import signal
import sys
from collections.abc import Sequence
from datetime import datetime

from dotenv import load_dotenv
from loguru import logger

from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator
from src.aqi.calculator import AQICalculator
from src.database.mongodb import MongoDBClient
from src.datadog.integrator import DatadogIntegrator
from src.pipeline import Pipeline, PipelineRunResult
from src.self_healing.engine import SelfHealingEngine
from src.self_healing.quarantine import QuarantineManager

load_dotenv()

# rotation renames per-day files; retention prunes old ones so the logs/
# directory does not grow without bound on a long-running deployment.
LOG_RETENTION_DAYS = int(os.getenv("LOG_RETENTION_DAYS", "14"))
logger.add(
    "logs/pipeline_{time}.log",
    rotation="1 day",
    retention=f"{LOG_RETENTION_DAYS} days",
    level=os.getenv("LOG_LEVEL", "INFO"),
)

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
    parser.add_argument(
        "--loop",
        action="store_true",
        help=(
            "Run continuously: one cycle every FETCH_INTERVAL_SECONDS "
            "(default 300) until stopped with SIGTERM/SIGINT. "
            "For deployment; a single run is the default."
        ),
    )
    return parser.parse_args(
        list(argv) if argv is not None else sys.argv[1:]
    )


def build_pipeline(
    database: MongoDBClient,
) -> tuple[Pipeline, SelfHealingEngine]:
    """Wire the fetcher, validator, database, and self-healing engine.

    The quarantine store is always wired to the real MongoDB collection.
    Refusing to start (fail-fast) when it is missing guarantees the
    production path can never fall back to the in-memory store, where
    quarantined records would be lost on restart. The in-memory mode
    remains available for dev/tests that build a manager directly.
    """
    quarantine_collection = database.quarantine_events
    if quarantine_collection is None:
        raise RuntimeError(
            "Quarantine store is not wired to MongoDB; refusing to start "
            "the pipeline without persistent quarantine."
        )
    fetcher = DataFetcher()
    validator = DataValidator()
    # Single source of truth: both write paths use the same retention
    # (database.quarantine_retention_days, read from .env at construction).
    # Previously the manager silently used its 30-day default while the DB
    # layer used 7.
    self_healing_engine = SelfHealingEngine(
        quarantine_manager=QuarantineManager(
            collection=quarantine_collection,
            retention_days=database.quarantine_retention_days,
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


def prune_expired_quarantine(pipeline: Pipeline) -> None:
    """Delete quarantined records past their retention TTL.

    The manager's cleanup() is a bounded delete_many; running it before
    every cycle keeps quarantine_events from growing without bound while
    never taking the pipeline down with a failure of its own.
    """
    try:
        deleted = pipeline.self_healing_engine.quarantine_manager.cleanup()
        if deleted:
            logger.info("Pruned {} expired quarantine record(s)", deleted)
    except Exception:
        logger.exception("Quarantine cleanup failed; continuing anyway.")


async def run_cycle(
    database: MongoDBClient,
    datadog: DatadogIntegrator | None,
    pipeline: Pipeline,
    location_id: int,
) -> PipelineRunResult:
    """Run one fetch → validate → persist → observe cycle.

    Used by both single-run mode and the scheduler so telemetry, logging
    and pipeline_status updates live in exactly one place.
    """
    prune_expired_quarantine(pipeline)
    result = await pipeline.run_once({"location_id": location_id})
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

    # Incidents of the current cycle only: run_once() resets the engine's
    # incident list at the start of each run.
    incidents = [
        inc.to_dict() for inc in pipeline.self_healing_engine.incidents
    ]
    metrics: dict[str, int | float] = {
        "pipeline.records.fetched": result.records_fetched,
        "pipeline.records.valid": result.records_valid,
        "pipeline.records.inserted": result.records_inserted,
        "pipeline.records.failed": result.records_failed,
        "pipeline.fetch.latency": result.duration_seconds,
        "pipeline.incidents.count": len(incidents),
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
        except Exception as error:
            # Telemetry must never take the pipeline down with it.
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
                    "incidents": len(incidents),
                },
                status="error" if incidents else "info",
            )
            logger.info("Datadog accepted pipeline log.")
        except Exception as error:
            logger.exception("Datadog log submission failed: {}", error)
        for incident in incidents:
            # Datadog logs need JSON-safe values: httpx's encoder does not
            # serialise datetime objects.
            created_at = incident["created_at"]
            created_at = (
                created_at.isoformat()
                if isinstance(created_at, datetime)
                else str(created_at)
            )
            try:
                datadog.send_logs(
                    {
                        "message": (
                            f"Pipeline incident on {incident['component']}: "
                            f"{incident['error']} "
                            f"(retryable={incident['retryable']})"
                        ),
                        "incident_id": incident["incident_id"],
                        "error_type": incident["error_type"],
                        "component": incident["component"],
                        "retryable": incident["retryable"],
                        "attempt": incident["attempt"],
                        "location_id": result.location_id,
                        "created_at": created_at,
                    },
                    status="error",
                )
            except Exception as error:
                logger.exception(
                    "Datadog incident log failed for {}: {}",
                    incident.get("incident_id"),
                    error,
                )

    # Cập nhật pipeline status vào MongoDB (2.4)
    database.update_pipeline_status(
        result.status,
        metrics=metrics,
        incidents=incidents,
    )
    return result


async def run_loop(
    database: MongoDBClient,
    datadog: DatadogIntegrator | None,
    pipeline: Pipeline,
    location_id: int,
    interval_seconds: int | float | None = None,
    stop_event: asyncio.Event | None = None,
    max_cycles: int | None = None,
) -> None:
    """Run the pipeline every ``interval_seconds`` until stopped.

    Graceful shutdown: SIGTERM/SIGINT only set the stop event so the cycle
    in progress finishes first. A failing cycle is logged and swallowed —
    the scheduler itself never dies, which is what keeps the pipeline
    self-healing. When ``stop_event`` is supplied, signal handlers are not
    installed (that keeps the loop testable).

    ``max_cycles`` bounds the loop for smoke tests; ``None`` runs until
    a stop request arrives.
    """
    interval = (
        interval_seconds
        if interval_seconds is not None
        else int(os.getenv("FETCH_INTERVAL_SECONDS", "300"))
    )
    injected_event = stop_event is not None
    if stop_event is None:
        stop_event = asyncio.Event()

    running_loop = asyncio.get_running_loop()
    installed_handlers: list[signal.Signals] = []

    def stop_on_signal(sig: signal.Signals) -> None:
        logger.info(
            "Received {}; scheduler will stop after the current cycle", sig
        )
        stop_event.set()

    if not injected_event:
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                running_loop.add_signal_handler(sig, stop_on_signal, sig)
                installed_handlers.append(sig)
            except (NotImplementedError, RuntimeError):
                # Signals are not manageable on every platform.
                pass

    logger.info(
        "Scheduler starting: location_id={} interval={}s",
        location_id,
        interval,
    )
    cycles = 0
    while not stop_event.is_set():
        try:
            await run_cycle(database, datadog, pipeline, location_id)
        except Exception:
            logger.exception(
                "Pipeline cycle failed unexpectedly; continuing the loop"
            )
        cycles += 1
        if (max_cycles is not None and cycles >= max_cycles) or stop_event.is_set():
            break
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval)
        except asyncio.TimeoutError:
            pass

    for sig in installed_handlers:
        try:
            running_loop.remove_signal_handler(sig)
        except (NotImplementedError, RuntimeError):
            pass

    logger.info("Scheduler stopped after {} cycle(s)", cycles)
    try:
        database.update_pipeline_status("stopped")
    except Exception:
        logger.exception("Could not record the stopped status in MongoDB.")


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
        if args.loop:
            await run_loop(database, datadog, pipeline, args.location_id)
        else:
            await run_cycle(database, datadog, pipeline, args.location_id)
    finally:
        if datadog is not None:
            datadog.close()
        database.close()


if __name__ == "__main__":
    asyncio.run(main())