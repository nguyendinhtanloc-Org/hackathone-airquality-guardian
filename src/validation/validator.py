"""
Data Validator - Kiểm tra dữ liệu
"""

from models.sensor_data import Measurement
from loguru import logger


class DataValidator:
    """Validates air quality data từ OpenAQ v3"""
    
    VALID_PARAMS = {"pm25", "pm10", "o3", "no2", "co", "so2"}
    
    def validate(self, api_response: dict) -> list[Measurement]:
        """
        Validate API response
        """
        valid = []
        results = api_response.get("results", [])
        
        for item in results:
            try:
                value = float(item.get("value", 0))
                parameter = item.get("parameter_name", "unknown")
                unit = item.get("unit", "")
                timestamp = item.get("datetime", {}).get("utc", "")
                
                # Chỉ giữ parameters hợp lệ
                if parameter.lower() in self.VALID_PARAMS:
                    measurement = Measurement(
                        parameter=parameter.lower(),
                        value=value,
                        unit=unit,
                        last_updated=timestamp
                    )
                    valid.append(measurement)
                else:
                    logger.debug(f"Skipped: {parameter}")
                    
            except Exception as e:
                logger.warning(f"Failed to parse: {e}")
        
        logger.info(f"Valid: {len(valid)}, Invalid: {len(results) - len(valid)}")
        return valid