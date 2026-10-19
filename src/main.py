"""
AirQuality Guardian - Main Entry Point
"""

import asyncio
import os
from dotenv import load_dotenv
from loguru import logger

from src.database.mongodb import MongoDBClient
from src.fetcher.fetcher import DataFetcher
from src.pipeline import Pipeline
from src.validation.validator import DataValidator

load_dotenv()

logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))


async def main() -> None:
    logger.info("Starting AirQuality Guardian Pipeline")

    database = MongoDBClient()
    fetcher = DataFetcher()
    validator = DataValidator()
    pipeline = Pipeline(fetcher, validator, database)
    try:
        inserted_count = await pipeline.run_once({"location_id": 4946813})
        logger.info(f"Pipeline completed; inserted {inserted_count} measurements")
    finally:
        database.close()


if __name__ == "__main__":
    asyncio.run(main())