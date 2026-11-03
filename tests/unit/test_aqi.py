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
    """Tham số ngoài danh sách chất ô nhiễm -> None"""
    aqi = calc.calculate("voc", 500.0)
    assert aqi is None


# ---------------------------------------------------------------------------
# CO (EPA: ppm, 8h) và SO2 (EPA: ppb, 1h)
# Nguồn: EPA Technical Assistance Document for the Reporting of Daily Air
# Quality - the AQI (EPA-454/B-18-007). Pipeline ingest µg/m³ nên phải
# quy đổi: 1 ppm CO = 1145 µg/m³; 1 ppb SO2 = 2.62 µg/m³.
# ---------------------------------------------------------------------------

CO_PPM_TO_AQI = [
    (0.0, 0),
    (4.4, 50),
    (4.5, 51),
    (9.4, 100),
    (9.5, 101),
    (12.4, 150),
    (12.5, 151),
    (15.4, 200),
    (15.5, 201),
    (30.4, 300),
    (30.5, 301),
    (40.4, 400),
    (40.5, 401),
    (50.4, 500),
]

SO2_PPB_TO_AQI = [
    (0, 0),
    (35, 50),
    (36, 51),
    (75, 100),
    (76, 101),
    (185, 150),
    (186, 151),
    (304, 200),
    (305, 201),
    (604, 300),
    (605, 301),
    (804, 400),
    (805, 401),
    (1004, 500),
]


@pytest.mark.parametrize("ppm,expected", CO_PPM_TO_AQI)
def test_calculate_co_at_every_epa_breakpoint(calc, ppm, expected):
    """CO tính theo ppm (đơn vị native của bảng EPA)."""
    assert calc.calculate("co", ppm, "ppm") == expected


@pytest.mark.parametrize("ppm,expected", CO_PPM_TO_AQI)
def test_calculate_co_from_ug_m3_matches_ppm_result(calc, ppm, expected):
    """Cùng một lượng CO đọc bằng µg/m³ (đơn vị pipeline ingest) cho
    cùng AQI - tức hệ số quy đổi đúng."""
    assert calc.calculate("co", ppm * 1145.0, "µg/m³") == expected


@pytest.mark.parametrize("ppb,expected", SO2_PPB_TO_AQI)
def test_calculate_so2_at_every_epa_breakpoint(calc, ppb, expected):
    """SO2 tính theo ppb (đơn vị native của bảng EPA)."""
    assert calc.calculate("so2", ppb, "ppb") == expected


@pytest.mark.parametrize("ppb,expected", SO2_PPB_TO_AQI)
def test_calculate_so2_from_ug_m3_matches_ppb_result(calc, ppb, expected):
    """SO2 đọc bằng µg/m³ cho cùng AQI với giá trị ppb."""
    assert calc.calculate("so2", ppb * 2.62, "µg/m³") == expected


def test_calculate_gas_without_unit_assumes_ug_m3(calc):
    """Không truyền đơn vị -> mặc định µg/m³ (đơn vị OpenAQ trả về)."""
    assert calc.calculate("co", 4.4 * 1145.0) == calc.calculate("co", 4.4, "ppm")
    assert calc.calculate("so2", 35 * 2.62) == calc.calculate("so2", 35, "ppb")


def test_calculate_gas_value_in_gap_between_bands_is_none(calc):
    """EPA để trống giữa các dải (4.5-9.4 là 1 dải, 4.45 không rơi vào
    dải nào) -> None, không phải số bịa."""
    assert calc.calculate("co", 4.45, "ppm") is None


def test_calculate_gas_above_table_is_none(calc):
    """Vượt trên ngưỡng cao nhất của bảng -> None."""
    assert calc.calculate("co", 60.0, "ppm") is None
    assert calc.calculate("so2", 2000, "ppb") is None


def test_calculate_pm_params_unaffected_by_gas_support(calc):
    """Thêm CO/SO2 không làm đổi hành vi của 4 chất bộ lọc bụi/khí cũ."""
    assert calc.calculate("pm25", 10.0, "µg/m³") == 42
    assert calc.calculate("pm25", 100.0, "µg/m³") == 174
    assert calc.calculate("no2", 100.0, "µg/m³") == 50


def test_live_station_co_reading_maps_to_low_aqi(calc):
    """co=984.57 µg/m³ (đọc thật ở trạm 4946813, ~0.86 ppm) -> AQI 10
    (Tốt). Trước khi thêm CO, giá trị này không có AQI nào."""
    assert calc.calculate("co", 984.57, "µg/m³") == 10


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