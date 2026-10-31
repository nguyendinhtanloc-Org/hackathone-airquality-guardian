"""
Data Validator - Kiem tra du lieu
"""

import math

from src.models.sensor_data import Measurement
from loguru import logger


class DataValidator:
    """Validates air quality data tu OpenAQ v3"""

    VALID_PARAMS = {"pm25", "pm10", "o3", "no2", "co", "so2"}

    # Physically plausible upper bound per parameter, in the unit OpenAQ
    # reports. A reading above this is a sensor fault, not real air.
    # (WHO guideline values are far lower; these are generous ceilings
    # that only catch obvious instrument errors.)
    # CO: urban background is ~1-3 ppm (1.2k-3.5k µg/m³), so the old
    # 100 µg/m³ ceiling was rejecting every normal reading - caught on
    # live data (co=984 µg/m³ dropped each cycle). 5000 µg/m³ (~4.3 ppm)
    # is still far above safe indoor levels, so only real faults pass.
    MAX_PLAUSIBLE = {
        "pm25": 1000.0,
        "pm10": 1500.0,
        "o3": 1000.0,
        "no2": 1000.0,
        "co": 5000.0,
        "so2": 1000.0,
    }

    def validate(self, api_response: dict) -> list[Measurement]:
        """
        Validate API response
        """
        valid = []
        results = api_response.get("results", [])

        for item in results:
            try:
                raw_value = item.get("value")
                if raw_value is None:
                    raise ValueError("missing value")
                value = float(raw_value)
                if not math.isfinite(value):
                    raise ValueError(f"non-finite value: {raw_value!r}")
                parameter = item.get("parameter_name", "unknown")
                unit = item.get("unit", "")
                timestamp = item.get("datetime", {}).get("utc", "")

                # Chi giu parameters hop le
                if parameter.lower() in self.VALID_PARAMS:
                    # Gia tri am hoac vuot nguong vat ly la loi sensor
                    ceiling = self.MAX_PLAUSIBLE[parameter.lower()]
                    if value < 0 or value > ceiling:
                        logger.warning(
                            "Implausible {} value {} (allowed 0..{})",
                            parameter,
                            value,
                            ceiling,
                        )
                        continue
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
