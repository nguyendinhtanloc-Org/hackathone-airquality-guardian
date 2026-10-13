"""
Data Models cho AirQuality Guardian
"""

from pydantic import BaseModel, Field
from typing import Optional, Literal
from datetime import datetime

class Measurement(BaseModel):
    """Một phép đo từ cảm biến"""

    parameter: str
    value: float = Field(ge=0)
    unit: str
    last_updated: Optional[str] = None

class SensorData(BaseModel):
    """Dữ liệu từ một trạm đo"""

    location_id: int
    location_name: str
    city: str
    country: str
    latitude: float
    longitude: float
    measurements: list[Measurement]
    datetime_utc: str

class AirQualityDocument(BaseModel):
    """Document lưu vào MongoDB"""

    location_id: int
    location_name: str
    city: str
    country: str
    latitude: float
    longitude: float
    timestamp: datetime
    pm25: Optional[float] = None
    pm10: Optional[float] = None
    o3: Optional[float] = None
    no2: Optional[float] = None
    aqi: Optional[int] = None
    aqi_level: Optional[str] = None
