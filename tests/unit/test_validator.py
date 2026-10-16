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