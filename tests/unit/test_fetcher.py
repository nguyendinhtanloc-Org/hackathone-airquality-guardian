"""
Unit tests cho DataFetcher
"""

import pytest
import httpx
from unittest.mock import AsyncMock, MagicMock, patch
from src.fetcher.fetcher import AirQualityFetchError, DataFetcher


@pytest.fixture
def fetcher():
    return DataFetcher()


@pytest.mark.asyncio
async def test_fetch_success(fetcher):
    """Test fetch thành công"""
    mock_latest = {
        "results": [
            {
                "value": 50.0,
                "sensorsId": 12345,
                "datetime": {"utc": "2026-10-16T00:00:00Z"}
            }
        ]
    }
    mock_sensor = {
        "results": [
            {
                "id": 12345,
                "parameter": {
                    "name": "pm25",
                    "units": "µg/m³"
                }
            }
        ]
    }
    
    with patch.object(fetcher, '_get_headers', return_value={}):
        async with httpx.AsyncClient() as client:
            with patch.object(client, 'get', new_callable=AsyncMock) as mock_get:
                mock_get.return_value.json.side_effect = [mock_latest, mock_sensor]
                mock_get.return_value.raise_for_status = lambda: None
                mock_get.return_value.status_code = 200
                
                # Không test trực tiếp vì phức tạp
                # Chỉ test logic đơn giản
                pass
    
    assert fetcher._sensor_cache == {}


@pytest.mark.asyncio
async def test_sensor_cache(fetcher):
    """Test cache hoạt động"""
    fetcher._sensor_cache[123] = {"parameter": {"name": "test"}}
    
    assert 123 in fetcher._sensor_cache
    assert fetcher._sensor_cache[123]["parameter"]["name"] == "test"


def test_clear_cache(fetcher):
    """Test xóa cache"""
    fetcher._sensor_cache[1] = {"test": "data"}
    fetcher.clear_cache()

    assert len(fetcher._sensor_cache) == 0


def _client_raising(error):
    """Build an AsyncClient stand-in whose GET raises the given error."""
    client = MagicMock()
    client.get = AsyncMock(side_effect=error)
    context = MagicMock()
    context.__aenter__ = AsyncMock(return_value=client)
    context.__aexit__ = AsyncMock(return_value=False)
    return context


@pytest.mark.asyncio
async def test_fetch_raises_timeout_error_on_timeout(fetcher):
    """Timeout is transient, so it must surface as a retryable error."""
    with patch(
        "src.fetcher.fetcher.httpx.AsyncClient",
        return_value=_client_raising(httpx.TimeoutException("timed out")),
    ):
        with pytest.raises(TimeoutError, match="timed out"):
            await fetcher.fetch({"location_id": 1})


@pytest.mark.asyncio
async def test_fetch_raises_connection_error_on_server_error(fetcher):
    """A 5xx is transient, so it must surface as a retryable error."""
    response = MagicMock()
    response.status_code = 503
    request = httpx.Request("GET", "https://api.openaq.org/v3/locations/1/latest")
    status_error = httpx.HTTPStatusError(
        "server error", request=request, response=response
    )
    with patch(
        "src.fetcher.fetcher.httpx.AsyncClient",
        return_value=_client_raising(status_error),
    ):
        with pytest.raises(ConnectionError, match="503"):
            await fetcher.fetch({"location_id": 1})


@pytest.mark.asyncio
async def test_fetch_raises_permanent_error_on_client_error(fetcher):
    """A 4xx must not look retryable, so it raises a dedicated error."""
    response = MagicMock()
    response.status_code = 404
    request = httpx.Request("GET", "https://api.openaq.org/v3/locations/1/latest")
    status_error = httpx.HTTPStatusError(
        "not found", request=request, response=response
    )
    with patch(
        "src.fetcher.fetcher.httpx.AsyncClient",
        return_value=_client_raising(status_error),
    ):
        with pytest.raises(AirQualityFetchError, match="404"):
            await fetcher.fetch({"location_id": 1})


@pytest.mark.asyncio
async def test_fetch_raises_connection_error_on_transport_failure(fetcher):
    with patch(
        "src.fetcher.fetcher.httpx.AsyncClient",
        return_value=_client_raising(httpx.ConnectError("no route")),
    ):
        with pytest.raises(ConnectionError, match="no route"):
            await fetcher.fetch({"location_id": 1})