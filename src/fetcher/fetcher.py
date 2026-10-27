"""
Data Fetcher - Lấy dữ liệu từ OpenAQ API
Có cache sensor info để tăng tốc
"""

import httpx
import asyncio
import os
from loguru import logger


class AirQualityFetchError(Exception):
    """A fetch failure that retrying cannot fix (bad request, not found, ...)."""


class DataFetcher:
    """Fetches air quality data from OpenAQ API v3"""
    
    def __init__(self, client_factory=None):
        self.base_url = "https://api.openaq.org/v3"
        self.api_key = os.getenv("OPENAQ_API_KEY")
        self.timeout = 10
        # Cache để không fetch lại sensor info
        self._sensor_cache: dict = {}
        # Injectable client factory so tests can supply a MockTransport.
        # Defaults to a real httpx.AsyncClient in production.
        self._client_factory = client_factory

    def _make_client(self) -> httpx.AsyncClient:
        if self._client_factory is not None:
            return self._client_factory()
        return httpx.AsyncClient(timeout=self.timeout)
    
    def _get_headers(self) -> dict:
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
    
    async def _get_sensor_info(self, client: httpx.AsyncClient, sensor_id: int) -> dict:
        """Lấy thông tin sensor (có cache)"""
        # Check cache trước
        if sensor_id in self._sensor_cache:
            return self._sensor_cache[sensor_id]
        
        url = f"{self.base_url}/sensors/{sensor_id}"
        try:
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                results = data.get("results", [])
                if results:
                    sensor_info = results[0]
                    self._sensor_cache[sensor_id] = sensor_info
                    return sensor_info
        except Exception as e:
            logger.warning(f"Failed to get sensor {sensor_id}: {e}")
        return {}
    
    async def fetch(self, params: dict = None) -> dict:
        """
        Fetch latest measurements.

        Raises TimeoutError or ConnectionError for transient failures that
        are worth retrying, and AirQualityFetchError for permanent ones.
        """
        params = params or {}
        location_id = params.get("location_id", 4946813)
        
        latest_url = f"{self.base_url}/locations/{location_id}/latest"
        
        try:
            async with self._make_client() as client:
                # Fetch measurements
                response = await client.get(latest_url, headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                
                results = data.get("results", [])
                logger.info(f"Got {len(results)} measurements")
                
                # Fetch sensor info (parallel + cache)
                sensor_ids = [m.get("sensorsId") for m in results if m.get("sensorsId")]
                # Chỉ fetch những sensor chưa có trong cache
                uncached_ids = [sid for sid in sensor_ids if sid not in self._sensor_cache]
                
                if uncached_ids:
                    logger.info(f"Fetching {len(uncached_ids)} new sensor info")
                    tasks = [self._get_sensor_info(client, sid) for sid in uncached_ids]
                    await asyncio.gather(*tasks)
                
                # Gắn sensor info vào measurements
                for m in results:
                    sensor_id = m.get("sensorsId")
                    if sensor_id in self._sensor_cache:
                        sensor_info = self._sensor_cache[sensor_id]
                        param = sensor_info.get("parameter", {})
                        if isinstance(param, dict):
                            m["parameter_name"] = param.get("name", "unknown")
                            m["unit"] = param.get("units", "")
                        else:
                            m["parameter_name"] = str(param) if param else "unknown"
                            m["unit"] = ""
                
                logger.info(f"Cache size: {len(self._sensor_cache)} sensors")
                return data
                
        except httpx.TimeoutException as error:
            logger.error(f"Request timeout: {error}")
            raise TimeoutError(f"OpenAQ request timed out: {error}") from error
        except httpx.HTTPStatusError as error:
            status_code = error.response.status_code
            if status_code >= 500:
                logger.error(f"OpenAQ server error {status_code}: {error}")
                raise ConnectionError(
                    f"OpenAQ server error {status_code}: {error}"
                ) from error
            logger.error(f"OpenAQ client error {status_code}: {error}")
            raise AirQualityFetchError(
                f"OpenAQ rejected the request with status {status_code}: {error}"
            ) from error
        except httpx.HTTPError as error:
            logger.error(f"HTTP error: {error}")
            raise ConnectionError(f"OpenAQ request failed: {error}") from error
    
    def clear_cache(self):
        """Xóa cache"""
        self._sensor_cache.clear()