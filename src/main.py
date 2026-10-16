"""
AirQuality Guardian - Main Entry Point
"""

"""
AirQuality Guardian - Main Entry Point
"""
import asyncio
import os
from dotenv import load_dotenv
from loguru import logger

from fetcher.fetcher import DataFetcher
from validation.validator import DataValidator

load_dotenv()

logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))


async def main():
    logger.info("Starting AirQuality Guardian Pipeline")
    
    fetcher = DataFetcher()
    validator = DataValidator()
    
    # Fetch từ location 4946813 (Hà Nội)
    data = await fetcher.fetch({"location_id": 4946813})
    
    if data:
        validated = validator.validate(data)
        logger.info(f"Fetched {len(validated)} valid measurements")
        
        for m in validated:
            print(f"  - {m.parameter}: {m.value} {m.unit}")
    else:
        logger.error("Failed to fetch data")


if __name__ == "__main__":
    asyncio.run(main())