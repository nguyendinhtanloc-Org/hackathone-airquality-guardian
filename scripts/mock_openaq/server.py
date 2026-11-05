"""Mock OpenAQ v3 service for deterministic self-healing demos.

Stdlib only (no deps) so it runs anywhere the pipeline's .env can be
pointed at: on the host, inside docker-compose, or as a pytest subprocess.

API shape mirrors OpenAQ v3:
  GET /v3/locations/{id}/latest -> {"results": [{value, sensorsId, datetime}]}
  GET /v3/sensors/{id}          -> {"results": [{id, parameter: {name, units}}]}

The six sensors mimic station 4946813:
  - pm25, pm10, o3, so2 are "fresh": each call advances their timestamp
    and value, so every fetch yields NEW documents (inserts) until a fault
    blocks the cycle.
  - no2, co are "stuck": fixed timestamp + value, mirroring the real
    station where no2/co stopped emitting new readings. After the first
    insert they are skipped by the dedup gate, exactly like production.

Fault switch (the demo lever):
  POST /control {"fault": "ok" | "500" | "401" | "timeout", "delay": 15}
  GET  /control  -> current state (for status checks / UIs)

    500   -> /latest answers 503 (pipeline retries 3x, then degrades
             and quarantines the cycle)
    401   -> /latest answers 401 (pipeline raises a permanent
             AirQualityFetchError -> degrades without retrying)
    timeout -> /latest hangs for `delay` seconds; httpx clients with a
             shorter timeout raise TimeoutError -> same degraded +
             quarantine path as 5xx
    ok    -> back to fresh readings

Run it:
    python scripts/mock_openaq/server.py            # default 127.0.0.1:8000
    MOCK_OPENAQ_PORT=8001 python scripts/mock_openaq/server.py
    MOCK_OPENAQ_HOST=0.0.0.0 python scripts/mock_openaq/server.py   # for Docker
"""

import json
import os
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STATION = {
    "id": 4946813,
    "name": "Ho Chi Minh City - District 1 (mock of OpenAQ 4946813)",
    "country_code": "VN",
}

# sensorsId -> (parameter, unit, kind)
SENSORS = {
    101: ("pm25", "µg/m³", "fresh"),
    102: ("pm10", "µg/m³", "fresh"),
    103: ("o3", "µg/m³", "fresh"),
    104: ("so2", "µg/m³", "fresh"),
    105: ("no2", "µg/m³", "stuck"),
    106: ("co", "µg/m³", "stuck"),
}

_LOCK = threading.Lock()

# Fresh sensors drift with each poll: value wobbles, timestamp advances
# one minute — enough for every cycle to produce a NEW dedup key.
_FRESH_SIDS = [sid for sid, (_, _, kind) in SENSORS.items() if kind == "fresh"]
_STATE = {
    "cycle": 0,
    "fault": "ok",
    "timeout_delay": 15,
    "values": {sid: 25.0 for sid in _FRESH_SIDS},
    "timestamps": {
        sid: time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        for sid in _FRESH_SIDS
    },
}

# Stuck sensors: pinned to a fixed "last seen" reading, mirroring the real
# station where no2/co stopped emitting new data. Their timestamp never
# moves, so after the first insert the dedup gate skips them again on
# every later cycle — the same skipped_dedup=2 behaviour as production.
_STUCK = {
    sid: {
        "value": 984.57 if p == "co" else 42.0,
        "datetime": "2026-04-24T00:00:00Z",
    }
    for sid, (p, _, k) in SENSORS.items()
    if k == "stuck"
}


def _latest_payload() -> dict:
    results = []
    for sid, (param, unit, kind) in SENSORS.items():
        if kind == "stuck":
            reading = _STUCK[sid]
        else:
            reading = {
                "value": round(_STATE["values"][sid] + 0.37, 2),
                "datetime": _STATE["timestamps"][sid],
            }
        results.append(
            {
                "value": reading["value"],
                "sensorsId": sid,
                "datetime": {"utc": reading["datetime"], "local": reading["datetime"]},
            }
        )
    return {"results": results, "meta": {"type": "readings", "count": len(results)}}


def _advance_fresh_sensors() -> None:
    """Fresh sensors emit a new reading on every successful poll."""
    _STATE["cycle"] += 1
    for sid, (_, _, kind) in SENSORS.items():
        if kind == "fresh":
            _STATE["values"][sid] = round(_STATE["values"][sid] * 1.03, 2)
            _STATE["timestamps"][sid] = time.strftime(
                "%Y-%m-%dT%H:%M:%SZ", time.gmtime()
            )


class _MockOpenAQHandler(BaseHTTPRequestHandler):
    server_version = "MockOpenAQ/1.0"

    def log_message(self, *args):  # keep the demo quiet
        pass

    def _send(self, status: int, payload: dict) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = self.path.split("?")[0]

        if path == "/control":
            with _LOCK:
                self._send(200, {"fault": _STATE["fault"],
                                 "timeout_delay": _STATE["timeout_delay"],
                                 "session": None})
            return

        if path == "/health":
            self._send(200, {"status": "ok"})
            return

        if path.startswith(_API_ROUTE_PREFIXES):
            self._handle_api()
        else:
            self._send(404, {"error": f"unknown route {path}"})

    def do_POST(self):
        if self.path.split("?")[0] != "/control":
            self._send(404, {"error": "unknown route"})
            return
        length = int(self.headers.get("Content-Length", 0))
        try:
            body = json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            self._send(400, {"error": "invalid json"})
            return
        fault = body.get("fault", "ok")
        if fault not in ("ok", "500", "401", "timeout"):
            self._send(400, {"error": f"unknown fault {fault!r}"})
            return
        with _LOCK:
            _STATE["fault"] = fault
            if "delay" in body:
                _STATE["timeout_delay"] = float(body["delay"])
            self._send(200, {"fault": fault, "timeout_delay": _STATE["timeout_delay"]})

    def _handle_api(self):
        with _LOCK:
            fault = _STATE["fault"]
            delay = _STATE["timeout_delay"]

        if fault == "timeout":
            # Hold the connection so httpx's (shorter) timeout fires first.
            time.sleep(delay)

        path = self.path.split("?")[0]
        if fault == "500":
            self._send(503, {"error": "upstream OpenAQ is down (mock fault)"})
            return
        if fault == "401":
            self._send(401, {"error": "unauthorized (mock fault)"})
            return

        if path.startswith("/v3/locations/") and path.endswith("/latest"):
            with _LOCK:
                _advance_fresh_sensors()
                self._send(200, _latest_payload())
            return

        if path.startswith("/v3/sensors/"):
            sensor_id = path.rsplit("/", 1)[-1]
            try:
                param, unit, _kind = SENSORS[int(sensor_id)]
            except (KeyError, ValueError):
                self._send(404, {"results": []})
                return
            self._send(
                200,
                {"results": [{"id": int(sensor_id), "parameter": {"name": param, "units": unit}}]},
            )
            return

        self._send(404, {"error": f"unknown api route {path}"})


_API_ROUTE_PREFIXES = ("/v3/locations/", "/v3/sensors/")


def make_server(port: int = 0, host: str = "127.0.0.1") -> ThreadingHTTPServer:
    return ThreadingHTTPServer((host, port), _MockOpenAQHandler)


def main() -> None:
    port = int(os.getenv("MOCK_OPENAQ_PORT", "8000"))
    server = make_server(port)
    print(f"Mock OpenAQ listening on http://127.0.0.1:{port}/v3")
    print('Fault switch: curl -X POST 127.0.0.1:{}/control -d \'{{"fault": "500"}}\''.format(port))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
