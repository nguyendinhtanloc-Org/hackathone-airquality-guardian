"""Interactive scenario simulator for the AirQuality Guardian pipeline.

Runs the REAL pipeline code (fetcher -> validator -> AQI -> persistence ->
self-healing -> Datadog telemetry) against a mocked OpenAQ API, so every
failure mode can be reproduced on demand and watched on the dashboard.

Usage:
    python scripts/simulate.py                 # interactive menu
    python scripts/simulate.py --scenario 3    # run one scenario directly
    python scripts/simulate.py --scenario all --no-datadog
    python scripts/simulate.py --mongo         # persist to the real MongoDB

Every metric/log is tagged ``scenario:<name>`` and ``source:simulator`` so the
dashboard's $scenario variable isolates simulated traffic from live data.
"""

from __future__ import annotations

import argparse
import asyncio
import math
import os
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import httpx
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
load_dotenv(ROOT / ".env")

from loguru import logger  # noqa: E402

from src.datadog.integrator import DatadogIntegrator  # noqa: E402
from src.fetcher.fetcher import DataFetcher  # noqa: E402
from src.main import run_cycle  # noqa: E402
from src.pipeline import Pipeline, PipelineRunResult  # noqa: E402
from src.self_healing.engine import SelfHealingEngine  # noqa: E402
from src.self_healing.quarantine import QuarantineManager  # noqa: E402
from src.validation.validator import DataValidator  # noqa: E402

LOCATION_ID = 4946813

# sensorsId -> (parameter, unit). Matches OpenAQ's /sensors/{id} shape.
SENSORS = {
    101: ("pm25", "µg/m³"),
    102: ("pm10", "µg/m³"),
    103: ("o3", "µg/m³"),
    104: ("no2", "µg/m³"),
    105: ("co", "µg/m³"),
    106: ("so2", "µg/m³"),
}
NORMAL_VALUES = {101: 35.0, 102: 60.0, 103: 40.0, 104: 30.0, 105: 900.0, 106: 10.0}

# --------------------------------------------------------------------------
# ANSI colours (Windows 10+ terminals support them)
# --------------------------------------------------------------------------
if os.name == "nt":
    os.system("")
C = {
    "b": "\033[1m", "dim": "\033[2m", "r": "\033[31m", "g": "\033[32m",
    "y": "\033[33m", "c": "\033[36m", "m": "\033[35m", "x": "\033[0m",
}


def paint(text: str, *styles: str) -> str:
    return "".join(C[s] for s in styles) + text + C["x"]


# --------------------------------------------------------------------------
# Mock OpenAQ API
# --------------------------------------------------------------------------
@dataclass
class MockOpenAQ:
    """Programmable OpenAQ: each call to /latest pops the next behaviour."""

    behaviours: list[Callable[[], httpx.Response]] = field(default_factory=list)
    default: Callable[[], httpx.Response] | None = None
    calls: int = 0

    def handler(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if "/sensors/" in path:
            sensor_id = int(path.rsplit("/", 1)[-1])
            param, unit = SENSORS.get(sensor_id, ("unknown", ""))
            return httpx.Response(
                200,
                json={"results": [{"id": sensor_id, "parameter": {"name": param, "units": unit}}]},
            )
        self.calls += 1
        behaviour = self.behaviours.pop(0) if self.behaviours else self.default
        return behaviour() if behaviour else ok_response()

    def client_factory(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(transport=httpx.MockTransport(self.handler))


_TICK = [0]


def _now_iso(offset_minutes: int = 0) -> str:
    # Each call gets a distinct second so successive cycles are never
    # accidentally deduplicated (only the duplicate scenario wants that).
    _TICK[0] += 1
    ts = datetime.now(timezone.utc) + timedelta(minutes=offset_minutes, seconds=_TICK[0])
    return ts.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def ok_response(values: dict[int, Any] | None = None, timestamp: str | None = None) -> httpx.Response:
    values = values if values is not None else NORMAL_VALUES
    ts = timestamp or _now_iso()
    results = [
        {"sensorsId": sid, "value": value, "datetime": {"utc": ts, "local": ts}}
        for sid, value in values.items()
    ]
    return httpx.Response(200, json={"meta": {"found": len(results)}, "results": results})


def timeout_response() -> httpx.Response:
    raise httpx.ReadTimeout("simulated OpenAQ timeout")


def network_down() -> httpx.Response:
    raise httpx.ConnectError("simulated network unreachable")


def status_response(code: int) -> Callable[[], httpx.Response]:
    return lambda: httpx.Response(code, json={"detail": f"simulated HTTP {code}"})


# --------------------------------------------------------------------------
# In-memory database (same interface the pipeline / run_cycle use)
# --------------------------------------------------------------------------
class MemoryDatabase:
    def __init__(self, quarantine: QuarantineManager) -> None:
        self.quarantine = quarantine
        self.quarantine_events = None
        self.quarantine_retention_days = 7
        self.docs: list[dict[str, Any]] = []
        self._keys: set[tuple] = set()
        self.status: dict[str, Any] = {}
        self.fail_writes = False

    def insert_air_quality(self, document: dict[str, Any]) -> Any:
        if self.fail_writes:
            raise RuntimeError("simulated MongoDB write failure (primary unavailable)")
        params = tuple(sorted(k for k in document["measurements"] if not k.endswith("_unit")))
        key = (document["metadata"]["location_id"], document["timestamp"], params)
        if key in self._keys:
            return None
        self._keys.add(key)
        self.docs.insert(0, document)
        return document["event_id"]

    def query_air_quality(self, filters: dict[str, Any] | None = None, limit: int = 100) -> list:
        return self.docs[:limit]

    def count_pending_quarantine(self) -> int:
        return sum(1 for r in self.quarantine._memory_store.values() if r.get("status") == "pending")

    def update_pipeline_status(self, status: str, metrics=None, incidents=None) -> None:
        self.status = {"status": status, "metrics": metrics, "incidents": incidents}

    def close(self) -> None:
        pass


class FailableMongo:
    """Proxy around the real MongoDBClient that can fail writes on demand."""

    def __init__(self, real: Any) -> None:
        self._real = real
        self.fail_writes = False

    def insert_air_quality(self, document: dict[str, Any]) -> Any:
        if self.fail_writes:
            raise RuntimeError("simulated MongoDB write failure (primary unavailable)")
        return self._real.insert_air_quality(document)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._real, name)


# --------------------------------------------------------------------------
# Simulation context
# --------------------------------------------------------------------------
@dataclass
class Context:
    database: Any
    datadog: DatadogIntegrator | None
    api: MockOpenAQ
    pipeline: Pipeline
    engine: SelfHealingEngine

    async def cycle(self, scenario: str) -> PipelineRunResult:
        return await run_cycle(
            self.database,
            self.datadog,
            self.pipeline,
            LOCATION_ID,
            extra_tags=[f"scenario:{scenario}", "source:simulator"],
        )


def build_context(use_mongo: bool, use_datadog: bool, backoff: float) -> Context:
    api = MockOpenAQ()
    if use_mongo:
        from src.database.mongodb import MongoDBClient

        real = MongoDBClient()
        database: Any = FailableMongo(real)
        quarantine = QuarantineManager(
            collection=real.quarantine_events,
            retention_days=real.quarantine_retention_days,
        )
    else:
        quarantine = QuarantineManager(retention_days=7)
        database = MemoryDatabase(quarantine)
    engine = SelfHealingEngine(
        quarantine_manager=quarantine,
        max_retries=int(os.getenv("MAX_RETRIES", "3")),
    )
    pipeline = Pipeline(
        DataFetcher(client_factory=api.client_factory),
        DataValidator(),
        database,
        self_healing_engine=engine,
        backoff_seconds=backoff,
    )
    datadog = DatadogIntegrator() if use_datadog and os.getenv("DATADOG_API_KEY") else None
    return Context(database, datadog, api, pipeline, engine)


def reset(ctx: Context) -> None:
    ctx.api.behaviours.clear()
    ctx.api.default = None
    ctx.api.calls = 0
    ctx.database.fail_writes = False


# --------------------------------------------------------------------------
# Scenarios
# --------------------------------------------------------------------------
async def sc_normal(ctx: Context) -> list[PipelineRunResult]:
    """Dữ liệu sạch, 6 chất ô nhiễm, insert thành công."""
    ctx.api.default = ok_response
    return [await ctx.cycle("normal")]


async def sc_transient_timeout(ctx: Context) -> list[PipelineRunResult]:
    """OpenAQ timeout 2 lần rồi hồi phục -> retry với exponential backoff."""
    ctx.api.behaviours = [timeout_response, network_down]
    ctx.api.default = ok_response
    return [await ctx.cycle("transient_timeout")]


async def sc_server_outage(ctx: Context) -> list[PipelineRunResult]:
    """OpenAQ trả 503 liên tục -> hết retry -> quarantine, status degraded."""
    ctx.api.default = status_response(503)
    return [await ctx.cycle("server_outage")]


async def sc_client_error(ctx: Context) -> list[PipelineRunResult]:
    """OpenAQ trả 401 (sai API key) -> lỗi vĩnh viễn, quarantine ngay, không retry."""
    ctx.api.default = status_response(401)
    return [await ctx.cycle("client_error_401")]


async def sc_bad_data(ctx: Context) -> list[PipelineRunResult]:
    """Sensor lỗi: giá trị âm, NaN, vượt ngưỡng, null -> validator loại bỏ."""
    values = {101: 42.0, 102: -5.0, 103: math.nan, 104: 99999.0, 105: None, 106: 12.0}
    ctx.api.default = lambda: httpx.Response(
        200,
        json={"results": [
            {"sensorsId": s, "value": (None if v is None else ("NaN" if isinstance(v, float) and math.isnan(v) else v)),
             "datetime": {"utc": _now_iso()}}
            for s, v in values.items()
        ]},
    )
    return [await ctx.cycle("bad_sensor_data")]


async def sc_empty(ctx: Context) -> list[PipelineRunResult]:
    """Trạm không có dữ liệu chu kỳ này -> healthy, 0 record."""
    ctx.api.default = lambda: httpx.Response(200, json={"results": []})
    return [await ctx.cycle("empty_response")]


async def sc_duplicate(ctx: Context) -> list[PipelineRunResult]:
    """OpenAQ trả lại cùng dữ liệu 2 lần -> dedup gate bỏ qua lần 2."""
    ts = _now_iso(-1)
    ctx.api.default = lambda: ok_response(timestamp=ts)
    return [await ctx.cycle("duplicate_data"), await ctx.cycle("duplicate_data")]


async def sc_db_failure(ctx: Context) -> list[PipelineRunResult]:
    """MongoDB không ghi được -> incident component=database, quarantine."""
    ctx.api.default = ok_response
    ctx.database.fail_writes = True
    return [await ctx.cycle("db_write_failure")]


async def sc_pollution_spike(ctx: Context) -> list[PipelineRunResult]:
    """Ô nhiễm nghiêm trọng (PM2.5 ~ 250) -> AQI Hazardous trên dashboard."""
    spike = {**NORMAL_VALUES, 101: 250.0, 102: 420.0, 104: 180.0}
    ctx.api.default = lambda: ok_response(spike)
    return [await ctx.cycle("pollution_spike")]


async def sc_outage_and_replay(ctx: Context) -> list[PipelineRunResult]:
    """Sự cố 503 -> quarantine -> API hồi phục -> replay dữ liệu bị cách ly."""
    from src.main import replay_quarantined

    ctx.api.default = status_response(503)
    results = [await ctx.cycle("outage_then_replay")]
    print(paint(f"  -> quarantine pending: {ctx.database.count_pending_quarantine()}", "y"))
    print(paint("  -> OpenAQ recovered, replaying quarantine...", "c"))
    ctx.api.default = ok_response
    summary = await replay_quarantined(ctx.pipeline, ctx.engine, limit=100)
    print(paint(f"  -> replay summary: {summary}", "g"))
    if ctx.datadog:
        try:
            ctx.datadog.send_metrics(
                {"selfhealing.replay.count": summary["succeeded"],
                 "selfhealing.quarantine.count": ctx.database.count_pending_quarantine()},
                tags=[f"env:{os.getenv('DD_ENV', 'dev')}", "service:airquality-pipeline",
                      f"location_id:{LOCATION_ID}", "scenario:outage_then_replay", "source:simulator"],
            )
        except Exception as error:  # noqa: BLE001
            logger.warning("Replay metric failed: {}", error)
    results.append(await ctx.cycle("outage_then_replay"))
    return results


async def sc_flapping(ctx: Context) -> list[PipelineRunResult]:
    """Chạy liên tục 6 chu kỳ, xen kẽ ngẫu nhiên OK/timeout/503/dữ liệu bẩn."""
    import random

    results = []
    for i in range(6):
        reset(ctx)
        pick = random.choice(["ok", "ok", "timeout", "503", "spike"])
        print(paint(f"  cycle {i + 1}/6: {pick}", "dim"))
        if pick == "timeout":
            ctx.api.behaviours = [timeout_response]
            ctx.api.default = ok_response
        elif pick == "503":
            ctx.api.default = status_response(503)
        elif pick == "spike":
            ctx.api.default = lambda: ok_response({**NORMAL_VALUES, 101: 180.0})
        else:
            ctx.api.default = ok_response
        results.append(await ctx.cycle("chaos_flapping"))
        print_result(results[-1], ctx.api.calls)
        await asyncio.sleep(1)
    return []


SCENARIOS: list[tuple[str, str, Callable]] = [
    ("normal", "Chạy bình thường", sc_normal),
    ("transient_timeout", "Timeout tạm thời -> retry thành công", sc_transient_timeout),
    ("server_outage", "OpenAQ sập (503) -> hết retry -> quarantine", sc_server_outage),
    ("client_error_401", "Sai API key (401) -> lỗi vĩnh viễn", sc_client_error),
    ("bad_sensor_data", "Dữ liệu sensor bẩn (âm/NaN/vượt ngưỡng)", sc_bad_data),
    ("empty_response", "Trạm không có dữ liệu", sc_empty),
    ("duplicate_data", "Dữ liệu trùng lặp -> dedup", sc_duplicate),
    ("db_write_failure", "MongoDB ghi lỗi", sc_db_failure),
    ("pollution_spike", "Ô nhiễm đột biến (AQI Hazardous)", sc_pollution_spike),
    ("outage_then_replay", "Sự cố -> hồi phục -> replay quarantine", sc_outage_and_replay),
    ("chaos_flapping", "Chaos: 6 chu kỳ lỗi ngẫu nhiên", sc_flapping),
]


# --------------------------------------------------------------------------
# Output
# --------------------------------------------------------------------------
def print_result(result: PipelineRunResult, api_calls: int) -> None:
    ok = result.status == "running"
    badge = paint(" OK ", "b", "g") if ok else paint(" DEGRADED ", "b", "r")
    print(f"  {badge} fetched={result.records_fetched} valid={result.records_valid} "
          f"inserted={paint(str(result.records_inserted), 'g')} "
          f"skipped={result.records_skipped} failed={paint(str(result.records_failed), 'r')} "
          f"attempts={result.attempts} api_calls={api_calls} "
          f"{result.duration_seconds:.2f}s")
    if result.incident:
        inc = result.incident
        print(paint(f"     incident: [{inc.component}] {inc.error_type}: {inc.error[:90]} "
                    f"(retryable={inc.retryable})", "y"))
    aqis = [v for d in result.documents for k, v in d["aqi"].items() if k.endswith("_aqi")]
    if aqis:
        print(paint(f"     max AQI this cycle: {max(aqis)}", "m"))


async def run_scenario(ctx: Context, index: int) -> None:
    key, title, fn = SCENARIOS[index]
    reset(ctx)
    print("\n" + paint(f"▶ [{index + 1}] {title}", "b", "c") + paint(f"  (scenario:{key})", "dim"))
    print(paint(f"  {fn.__doc__}", "dim"))
    results = await fn(ctx)
    for r in results:
        print_result(r, ctx.api.calls)
    print(paint(f"  quarantine pending: {ctx.database.count_pending_quarantine()}", "dim"))


def menu() -> str:
    print("\n" + paint("═" * 60, "c"))
    print(paint("  AirQuality Guardian — Scenario Simulator", "b"))
    print(paint("═" * 60, "c"))
    for i, (key, title, _) in enumerate(SCENARIOS, 1):
        print(f"  {paint(f'{i:>2}', 'b', 'y')}. {title}")
    print(f"  {paint(' a', 'b', 'y')}. Chạy tất cả kịch bản")
    print(f"  {paint(' q', 'b', 'y')}. Thoát")
    return input(paint("\nChọn kịch bản (vd: 1 hoặc 2,5,9): ", "b")).strip().lower()


def parse_choice(choice: str) -> list[int] | None:
    if choice in {"a", "all"}:
        return list(range(len(SCENARIOS)))
    picks = []
    for part in choice.replace(" ", "").split(","):
        if part.isdigit() and 1 <= int(part) <= len(SCENARIOS):
            picks.append(int(part) - 1)
        else:
            return None
    return picks or None


async def amain() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scenario", help="Số kịch bản (1..N), danh sách '1,3' hoặc 'all'.")
    parser.add_argument("--mongo", action="store_true", help="Ghi vào MongoDB thật thay vì in-memory.")
    parser.add_argument("--no-datadog", action="store_true", help="Không gửi metrics/logs lên Datadog.")
    parser.add_argument("--backoff", type=float, default=0.5, help="Retry backoff (giây), mặc định 0.5.")
    parser.add_argument("--verbose", action="store_true", help="Hiện log chi tiết của pipeline.")
    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stderr, level="DEBUG" if args.verbose else "CRITICAL")

    ctx = build_context(args.mongo, not args.no_datadog, args.backoff)
    print(paint(f"DB: {'MongoDB (real)' if args.mongo else 'in-memory'} | "
                f"Datadog: {'ON' if ctx.datadog else 'OFF'} | backoff: {args.backoff}s", "dim"))
    try:
        if args.scenario:
            picks = parse_choice(args.scenario.lower())
            if picks is None:
                print("Kịch bản không hợp lệ.")
                return 1
            for i in picks:
                await run_scenario(ctx, i)
            return 0
        while True:
            choice = menu()
            if choice in {"q", "quit", "exit"}:
                return 0
            picks = parse_choice(choice)
            if picks is None:
                print(paint("Lựa chọn không hợp lệ.", "r"))
                continue
            for i in picks:
                await run_scenario(ctx, i)
    finally:
        if ctx.datadog:
            ctx.datadog.close()
        ctx.database.close()


if __name__ == "__main__":
    try:
        raise SystemExit(asyncio.run(amain()))
    except KeyboardInterrupt:
        print("\nBye.")
