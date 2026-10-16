"""
Unit tests cho AQICalculator
"""

import pytest
from src.aqi.calculator import AQICalculator
from src.models.sensor_data import Measurement


@pytest.fixture
def calc():
    return AQICalculator()


def test_calculate_pm25_good(calc):
    """PM2.5 = 10 → AQI ~41 (Good)"""
    aqi = calc.calculate("pm25", 10.0)
    assert aqi is not None
    assert 0 <= aqi <= 50


def test_calculate_pm25_unhealthy(calc):
    """PM2.5 = 100 → AQI ~152 (Unhealthy)"""
    aqi = calc.calculate("pm25", 100.0)
    assert aqi is not None
    assert 150 <= aqi <= 200


def test_calculate_unsupported_param(calc):
    """CO không có trong breakpoints"""
    aqi = calc.calculate("co", 500.0)
    assert aqi is None


def test_get_level(calc):
    """Test AQI level"""
    level = calc.get_level(45)
    assert level["level"] == "good"
    assert level["color"] == "#00E400"
    
    level = calc.get_level(200)
    assert level["level"] == "unhealthy"
    assert level["color"] == "#FF0000"


def test_calculate_all(calc):
    """Test tính AQI cho nhiều parameters"""
    measurements = [
        Measurement(parameter="pm25", value=50.0, unit="µg/m³"),
        Measurement(parameter="o3", value=100.0, unit="µg/m³"),
    ]
    
    result = calc.calculate_all(measurements)
    
    assert result["primary_pollutant"] in ["pm25", "o3"]
    assert result["aqi"] > 0