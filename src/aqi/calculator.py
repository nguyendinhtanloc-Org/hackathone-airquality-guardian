"""
AQI Calculator - Tính AQI theo chuẩn EPA
"""

from loguru import logger


# EPA AQI Breakpoints cho PM2.5 (µg/m³)
PM25_BREAKPOINTS = [
    (0.0, 12.0, 0, 50),
    (12.1, 35.4, 51, 100),
    (35.5, 55.4, 101, 150),
    (55.5, 150.4, 151, 200),
    (150.5, 250.4, 201, 300),
    (250.5, 500.4, 301, 500),
]

# EPA AQI Breakpoints cho PM10 (µg/m³)
PM10_BREAKPOINTS = [
    (0, 54, 0, 50),
    (55, 154, 51, 100),
    (155, 254, 101, 150),
    (255, 354, 151, 200),
    (355, 424, 201, 300),
    (425, 604, 301, 500),
]

# EPA AQI Breakpoints cho O3 (µg/m³) - 8-hour
O3_BREAKPOINTS = [
    (0, 108, 0, 50),
    (109, 140, 51, 100),
    (141, 170, 101, 150),
    (171, 210, 151, 200),
    (211, 400, 201, 300),
    (401, 800, 301, 500),
]

# EPA AQI Breakpoints cho NO2 (µg/m³)
NO2_BREAKPOINTS = [
    (0, 100, 0, 50),
    (101, 188, 51, 100),
    (189, 677, 101, 150),
    (678, 1221, 151, 200),
    (1222, 2349, 201, 300),
    (2350, 3853, 301, 500),
]

# EPA AQI Breakpoints cho CO (ppm, trung bình 8 giờ).
# Nguồn: EPA, Technical Assistance Document for the Reporting of Daily
# Air Quality - the AQI (EPA-454/B-18-007), bảng breakpoints.
CO_BREAKPOINTS = [
    (0.0, 4.4, 0, 50),
    (4.5, 9.4, 51, 100),
    (9.5, 12.4, 101, 150),
    (12.5, 15.4, 151, 200),
    (15.5, 30.4, 201, 300),
    (30.5, 40.4, 301, 400),
    (40.5, 50.4, 401, 500),
]

# EPA AQI Breakpoints cho SO2 (ppb, trung bình 1 giờ).
SO2_BREAKPOINTS = [
    (0, 35, 0, 50),
    (36, 75, 51, 100),
    (76, 185, 101, 150),
    (186, 304, 151, 200),
    (305, 604, 201, 300),
    (605, 804, 301, 400),
    (805, 1004, 401, 500),
]

# Đơn vị "native" của bảng breakpoints cho từng chất khí, cùng hệ số
# quy đổi sang đơn vị đó. Hệ số lấy từ bảng conversion factors của EPA
# (25°C, 1 atm): 1 ppm CO = 1.145 mg/m³; 1 ppb SO2 = 2.620 µg/m³.
# Pipeline ingest µg/m³, nên phải quy đổi trước khi tra bảng.
GAS_UNITS = {
    "co": {
        "native": "ppm",
        "factors": {
            "ppm": 1.0,
            "ppb": 1e-3,
            # 1 ppm CO = 1.145 mg/m³ = 1145 µg/m³
            "mg/m3": 1 / 1.145,
            "ug/m3": 1 / 1145.0,
        },
    },
    "so2": {
        "native": "ppb",
        "factors": {
            "ppb": 1.0,
            "ppm": 1e3,
            # 1 ppb SO2 = 2.620 µg/m³  (1 ppm = 2.620 mg/m³)
            "mg/m3": 1 / 2.620,
            "ug/m3": 1 / 2.620,
        },
    },
}

# AQI Levels
AQI_LEVELS = [
    (0, 50, "good", "#00E400", "Tốt"),
    (51, 100, "moderate", "#FFFF00", "Trung bình"),
    (101, 150, "unhealthy_sensitive", "#FF7E00", "Không tốt cho nhóm nhạy cảm"),
    (151, 200, "unhealthy", "#FF0000", "Không tốt"),
    (201, 300, "very_unhealthy", "#8F3F97", "Rất không tốt"),
    (301, 500, "hazardous", "#7E0023", "Nguy hiểm"),
]

# Map parameter -> breakpoints
BREAKPOINTS_MAP = {
    "pm25": PM25_BREAKPOINTS,
    "pm10": PM10_BREAKPOINTS,
    "o3": O3_BREAKPOINTS,
    "no2": NO2_BREAKPOINTS,
    "co": CO_BREAKPOINTS,
    "so2": SO2_BREAKPOINTS,
}


class AQICalculator:
    """Tính AQI theo chuẩn EPA"""

    @staticmethod
    def _normalize_unit(unit: str | None) -> str | None:
        """Chuẩn hoá chuỗi đơn vị về dạng ASCII để tra bảng.

        OpenAQ trả "µg/m³" (ký tự micro + mũ 3), còn bảng hệ số quy đổi
        của EPA dùng "ug/m3". Cùng một đơn vị phải map về 1 key duy nhất.
        """
        if not unit:
            return None
        cleaned = (
            unit.strip().lower()
            .replace("µ", "u").replace("μ", "u")
            .replace("³", "3")
            .replace(" ", "")
        )
        return cleaned or None

    def _to_native(self, parameter: str, value: float, unit: str | None) -> float:
        """Quy đổi giá trị về đơn vị native của bảng breakpoints.

        Với pm25/pm10/o3/no2, bảng đã dùng µg/m³ nên không đổi. Với CO
        (native = ppm) và SO2 (native = ppb), giá trị phải được quy đổi
        từ đơn vị đo thực tế. Khi không biết đơn vị, mặc định µg/m³ —
        đó là đơn vị pipeline luôn ingest.
        """
        spec = GAS_UNITS.get(parameter)
        if spec is None:
            return value
        normalized = self._normalize_unit(unit) or "ug/m3"
        factor = spec["factors"].get(normalized)
        if factor is None:
            # Đơn vị lạ (ví dụ "mg/m³" viết kiểu khác) -> dùng µg/m³,
            # vì đó là đơn vị OpenAQ trả về cho mọi chất khí.
            factor = spec["factors"]["ug/m3"]
        return value * factor

    def calculate(
        self,
        parameter: str,
        value: float,
        unit: str | None = None,
    ) -> int | None:
        """
        Tính AQI cho một parameter

        Args:
            parameter: pm25, pm10, o3, no2, co, so2
            value: Giá trị đo, theo đơn vị của `unit` (mặc định µg/m³)
            unit: Đơn vị đo. Với CO/SO2, dùng để quy đổi sang đơn vị
                native của bảng EPA (ppm / ppb).

        Returns:
            AQI value (0-500) hoặc None nếu không tính được
        """
        param = parameter.lower()
        if param not in BREAKPOINTS_MAP:
            return None

        breakpoints = BREAKPOINTS_MAP[param]
        native_value = self._to_native(param, value, unit)

        for bp_lo, bp_hi, aqi_lo, aqi_hi in breakpoints:
            # Dung sai số: giá trị đúng ở biên bảng (ví dụ 30.4 ppm) có
            # thể lệch vài ulp sau khi quy đổi đơn vị, rơi vào khoảng
            # trống giữa 2 dải và trả về None. Cho phép số ở biên nằm
            # trong dải khi cách không quá 1e-9.
            eps = max(1e-9, abs(bp_hi) * 1e-12)
            if bp_lo - eps <= native_value <= bp_hi + eps:
                # Công thức EPA. Clamp vào dải [aqi_lo, aqi_hi]: một giá
                # trị đúng ở biên (ví dụ 30.4 ppm) có thể lệch vài ulp
                # sau quy đổi đơn vị và công thức sẽ ngoại suy vượt aqi_hi.
                aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (native_value - bp_lo) + aqi_lo
                aqi = min(max(aqi, aqi_lo), aqi_hi)
                return round(aqi)

        return None

    def get_level(self, aqi_value: int) -> dict:
        """
        Lấy mô tả AQI level
        
        Args:
            aqi_value: AQI đã tính
            
        Returns:
            Dict chứa level, color, description
        """
        for aqi_lo, aqi_hi, level, color, desc in AQI_LEVELS:
            if aqi_lo <= aqi_value <= aqi_hi:
                return {
                    "level": level,
                    "color": color,
                    "description": desc
                }
        
        return {
            "level": "unknown",
            "color": "#000000",
            "description": "N/A"
        }
    
    def calculate_all(self, measurements: list) -> dict:
        """
        Tính AQI cho tất cả measurements
        
        Args:
            measurements: List of Measurement objects

        Returns:
            Dict chứa AQI theo parameter và AQI tổng

        NOTE (audit): not called by the ingest pipeline (which uses the
        per-pollutant calculate() + get_level()); this is the summary
        primitive the 2.13 `--report` CLI task will consume - keep it.
        """
        aqi_values = {}
        
        for m in measurements:
            aqi = self.calculate(m.parameter, m.value, getattr(m, "unit", None))
            if aqi is not None:
                aqi_values[m.parameter] = {
                    "value": m.value,
                    "unit": m.unit,
                    "aqi": aqi
                }
        
        if not aqi_values:
            return {"primary_pollutant": None, "aqi": 0, "level": {}}
        
        # Lấy AQI cao nhất (primary pollutant)
        primary = max(aqi_values.items(), key=lambda x: x[1]["aqi"])
        primary_param = primary[0]
        primary_aqi = primary[1]["aqi"]
        
        level = self.get_level(primary_aqi)
        
        return {
            "primary_pollutant": primary_param,
            "aqi": primary_aqi,
            "level": level,
            "all": aqi_values
        }