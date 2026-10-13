"""
Data Validator - Kiểm tra dữ liệu
"""
from src.models.sensor_data import SensorData, Measurement
from loguru import logger


class DataValidator:
    """Validates air quality data"""
    
    VALID_PARAMS = {"pm25", "pm10", "o3", "no2", "co", "so2"}
    
    def validate(self, api_response: dict) -> list[SensorData]:
        """
        Validate API response, trả về list SensorData
        """
        valid = []
        results = api_response.get("results", [])
        
        for location in results:
            try:
                measurements = []
                for m in location.get("measurements", []):
                    param = m.get("parameter", "").lower()
                    if param in self.VALID_PARAMS:
                        measurements.append(Measurement(
                            parameter=param,
                            value=float(m.get("value", 0)),
                            unit=m.get("unit", ""),
                            last_updated=m.get("lastUpdated")
                        ))
                
                if measurements:
                    sensor_data = SensorData(
                        location_id=location.get("id"),
                        location_name=location.get("name", ""),
                        city=location.get("city", "Unknown"),
                        country=location.get("country", ""),
                        latitude=location.get("coordinates", {}).get("latitude", 0),
                        longitude=location.get("coordinates", {}).get("longitude", 0),
                        measurements=measurements,
                        datetime_utc=measurements[0].last_updated or ""
                    )
                    valid.append(sensor_data)
                    
            except Exception as e:
                logger.warning(f"Failed to parse: {location.get('name')}: {e}")
        
        logger.info(f"Valid: {len(valid)}, Invalid: {len(results) - len(valid)}")
        return valid