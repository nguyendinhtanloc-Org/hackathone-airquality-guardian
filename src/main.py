"""
AirQuality Guardian - Main Entry Point
"""
import asyncio
import os
from dotenv import load_dotenv
from loguru import logger

from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator

load_dotenv()

# Configure logging
logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))


async def main():
    logger.info("Starting AirQuality Guardian Pipeline")
    
    fetcher = DataFetcher()
    validator = DataValidator()
    
    # Test fetch
    data = await fetcher.fetch({"city": "Hanoi", "limit": 5})
    
    if data:
        validated = validator.validate(data)
        logger.info(f"Fetched {len(validated)} valid records")
        
        for v in validated:
            print(f"  - {v.location_name}")
    else:
        logger.error("Failed to fetch data")


if __name__ == "__main__":
    asyncio.run(main())