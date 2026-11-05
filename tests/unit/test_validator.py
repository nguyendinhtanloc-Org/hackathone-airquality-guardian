"""
Unit tests cho DataValidator
"""

import pytest
from src.validation.validator import DataValidator


@pytest.fixture
def validator():
    return DataValidator()


def test_validate_valid_measurements(validator):
    """Test validate với data hợp lệ"""
    api_response = {
        "results": [
            {
                "value": 50.0,
                "parameter_name": "pm25",
                "unit": "µg/m³",
                "datetime": {"utc": "2026-10-16T00:00:00Z"}
            },
            {
                "value": 30.0,
                "parameter_name": "o3",
                "unit": "µg/m³",
                "datetime": {"utc": "2026-10-16T00:00:00Z"}
            }
        ]
    }
    
    result = validator.validate(api_response)
    
    assert len(result) == 2
    assert result[0].parameter == "pm25"
    assert result[0].value == 50.0
    assert result[1].parameter == "o3"


def test_validate_skip_invalid_param(validator):
    """Test skip param không hợp lệ"""
    api_response = {
        "results": [
            {
                "value": 50.0,
                "parameter_name": "pm25",
                "unit": "µg/m³",
                "datetime": {"utc": "2026-10-16T00:00:00Z"}
            },
            {
                "value": 30.0,
                "parameter_name": "temperature",  # Không hợp lệ
                "unit": "C",
                "datetime": {"utc": "2026-10-16T00:00:00Z"}
            }
        ]
    }
    
    result = validator.validate(api_response)
    
    assert len(result) == 1
    assert result[0].parameter == "pm25"


def test_validate_empty(validator):
    """Test empty results"""
    api_response = {"results": []}
    result = validator.validate(api_response)

    assert len(result) == 0


def _row(value, parameter="co"):
    return {
        "value": value,
        "parameter_name": parameter,
        "unit": "µg/m³",
        "datetime": {"utc": "2026-10-28T11:40:00Z"},
    }


def test_validator_keeps_normal_urban_co_reading(validator):
    """Live-data regression: co=984.57 µg/m³ (~0.86 ppm) is normal urban
    background, but the old 100 µg/m³ ceiling dropped it every cycle.
    The ceiling must only catch true sensor faults, not real air."""
    result = validator.validate({"results": [_row(984.57)]})
    assert len(result) == 1
    assert result[0].parameter == "co"
    assert result[0].value == 984.57


def test_validator_still_rejects_absurd_co_fault(validator):
    """Above ~13 ppm (1.5x the WHO 8-hour guideline) nothing in air
    quality monitoring is plausible; the ceiling must still catch
    instrument faults."""
    assert validator.validate({"results": [_row(20000.0)]}) == []


def test_validator_keeps_co_in_epa_hazardous_range(validator):
    """CO must reach the AQI range where it matters: EPA AQI-100 starts
    at 9.4 ppm = 10 763 µg/m³. The old 5000 µg/m³ ceiling sat exactly
    on EPA's AQI-50 line, so a genuinely polluted reading was dropped
    as a sensor fault and the CO AQI could never leave the "Good" band.
    """
    polluted = 9.4 * 1145.0  # 10 763 µg/m³
    result = validator.validate({"results": [_row(polluted)]})
    assert len(result) == 1
    assert result[0].value == polluted


def test_validator_keeps_so2_up_to_epa_table_top(validator):
    """EPA's SO2 table tops out at 1004 ppb = 2630 µg/m³; the old
    1000 µg/m³ ceiling cut the top AQI bands off entirely."""
    result = validator.validate({"results": [_row(2630.0, parameter="so2")]})
    assert len(result) == 1
    assert result[0].parameter == "so2"