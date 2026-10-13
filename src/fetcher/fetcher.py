"""
Data Fetcher - Lấy dữ liệu từ OpenAQ API
"""
import httpx
import asyncio
import os
from loguru import logger


class DataFetcher:
    """Fetches air quality data from OpenAQ API"""
    
    def __init__(self):
        self.api_url = "https://api.openaq.org/v2/latest"
        self.api_key = os.getenv("OPENAQ_API_KEY")
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.timeout = 10
    
    async def fetch(self, params: dict = None) -> dict | None:
        """
        Fetch data từ OpenAQ API với retry
        """

        params = params or {}
        
        for attempt in range(self.max_retries):
            try:
                headers = {}
                if self.api_key:
                    headers["X-API-Key"] = self.api_key
                
                async with httpx.AsyncClient(timeout=self.timeout) as client:
                    response = await client.get(self.api_url, params=params, headers=headers)
                    response.raise_for_status()
                    
                    logger.info(f"Fetched data: {response.status_code}")
                    return response.json()
                    
            except httpx.TimeoutException:
                logger.warning(f"Timeout at attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(30 * (attempt + 1))
                    
            except httpx.HTTPError as e:
                logger.error(f"HTTP error: {e}")

                return None
        
        logger.error("Max retries exceeded")
        
        return None