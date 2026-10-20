"""
AirQuality Guardian - Main Entry Point
"""

import asyncio
import os
from dotenv import load_dotenv
from loguru import logger

from src.fetcher.fetcher import DataFetcher
from src.validation.validator import DataValidator
from src.aqi.calculator import AQICalculator

load_dotenv()

logger.add("logs/pipeline_{time}.log", rotation="1 day", level=os.getenv("LOG_LEVEL", "INFO"))


async def main():
    logger.info("Starting AirQuality Guardian Pipeline")
    
    fetcher = DataFetcher()
    validator = DataValidator()
    aqi_calc = AQICalculator()
    
    # Fetch từ location 4946813 (Hà Nội)
    data = await fetcher.fetch({"location_id": 4946813})
    
    if data:
        validated = validator.validate(data)
        logger.info(f"Fetched {len(validated)} valid measurements")
        
        # In measurements
        for m in validated:
            print(f"  - {m.parameter}: {m.value} {m.unit}")
        
        # Tính AQI
        aqi_result = aqi_calc.calculate_all(validated)
        
        print(f"\n--- AQI Result ---")
        print(f"Primary Pollutant: {aqi_result['primary_pollutant']}")
        print(f"AQI: {aqi_result['aqi']}")
        print(f"Level: {aqi_result['level']['description']} ({aqi_result['level']['level']})")
        print(f"Color: {aqi_result['level']['color']}")
        
    else:
        logger.error("Failed to fetch data")


if __name__ == "__main__":
    asyncio.run(main())