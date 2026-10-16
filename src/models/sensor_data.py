"""
Data Models cho AirQuality Guardian
"""

from pydantic import BaseModel, Field
from typing import Optional, List

class Measurement(BaseModel):
    """Một phép đo từ cảm biến"""
    parameter: str
    value: float = Field(default=0, ge=0)
    unit: str = ""
    last_updated: Optional[str] = None


class SensorData(BaseModel):
    """Dữ liệu từ một trạm đo"""
    location_id: int
    location_name: str
    city: str
    country: str
    latitude: float = 0
    longitude: float = 0
    measurements: List[Measurement] = []
    datetime_utc: str = ""