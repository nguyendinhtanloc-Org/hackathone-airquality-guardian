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
}


class AQICalculator:
    """Tính AQI theo chuẩn EPA"""
    
    def calculate(self, parameter: str, value: float) -> int | None:
        """
        Tính AQI cho một parameter
        
        Args:
            parameter: pm25, pm10, o3, no2
            value: Giá trị đo (µg/m³)
            
        Returns:
            AQI value (0-500) hoặc None nếu không tính được
        """
        param = parameter.lower()
        if param not in BREAKPOINTS_MAP:
            return None
        
        breakpoints = BREAKPOINTS_MAP[param]
        
        for bp_lo, bp_hi, aqi_lo, aqi_hi in breakpoints:
            if bp_lo <= value <= bp_hi:
                # Công thức EPA
                aqi = ((aqi_hi - aqi_lo) / (bp_hi - bp_lo)) * (value - bp_lo) + aqi_lo
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
        """
        aqi_values = {}
        
        for m in measurements:
            aqi = self.calculate(m.parameter, m.value)
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