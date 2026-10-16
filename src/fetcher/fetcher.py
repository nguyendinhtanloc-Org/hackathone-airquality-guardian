"""
Data Fetcher - Lấy dữ liệu từ OpenAQ API
"""

import httpx
import os
from loguru import logger


class DataFetcher:
    """Fetches air quality data from OpenAQ API v3"""
    
    def __init__(self):
        self.base_url = "https://api.openaq.org/v3"
        self.api_key = os.getenv("OPENAQ_API_KEY")
        self.max_retries = int(os.getenv("MAX_RETRIES", "3"))
        self.timeout = 10
    
    def _get_headers(self) -> dict:
        headers = {"accept": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key
        return headers
    
    async def _get_sensor_info(self, client: httpx.AsyncClient, sensor_id: int) -> dict:
        """
        Lấy thông tin sensor từ /v3/sensors/{id}
        Response có dạng: {"meta": ..., "results": [{sensor_info}]}
        """
        url = f"{self.base_url}/sensors/{sensor_id}"
        try:
            response = await client.get(url, headers=self._get_headers())
            if response.status_code == 200:
                data = response.json()
                # Lấy sensor đầu tiên trong results
                results = data.get("results", [])
                if results:
                    return results[0]  # Trả về sensor info
        except Exception as e:
            logger.warning(f"Failed to get sensor {sensor_id}: {e}")
        return {}
    
    async def fetch(self, params: dict = None) -> dict | None:
        """
        Fetch latest measurements
        """
        params = params or {}
        location_id = params.get("location_id", 4946813)
        
        latest_url = f"{self.base_url}/locations/{location_id}/latest"
        
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                # Bước 1: Lấy measurements
                response = await client.get(latest_url, headers=self._get_headers())
                response.raise_for_status()
                data = response.json()
                
                results = data.get("results", [])
                logger.info(f"Got {len(results)} measurements")
                
                # Bước 2: Với mỗi measurement, fetch sensor info riêng
                for m in results:
                    sensor_id = m.get("sensorsId")
                    if sensor_id:
                        sensor_info = await self._get_sensor_info(client, sensor_id)
                        # sensor_info là dict có "parameter" field
                        param = sensor_info.get("parameter", {})
                        if isinstance(param, dict):
                            m["parameter_name"] = param.get("name", "unknown")
                            m["unit"] = param.get("units", "")
                        else:
                            m["parameter_name"] = str(param) if param else "unknown"
                            m["unit"] = ""
                        
                        logger.info(
                            f"  sensor {sensor_id} -> {m['parameter_name']} "
                            f"= {m.get('value')} {m.get('unit', '')}"
                        )
                
                return data
                
        except httpx.TimeoutException:
            logger.error("Request timeout")
            return None
        except httpx.HTTPError as e:
            logger.error(f"HTTP error: {e}")
            return None