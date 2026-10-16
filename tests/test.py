"""
Debug - Xem response thật từ /v3/sensors/{id}
"""
import asyncio
import httpx
import os
import json
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("OPENAQ_API_KEY")
BASE_URL = "https://api.openaq.org/v3"

headers = {"accept": "application/json"}
if API_KEY:
    headers["X-API-Key"] = API_KEY


async def test_sensor(sensor_id: int):
    """Test một sensor ID"""
    print(f"\n{'='*60}")
    print(f"TEST: /v3/sensors/{sensor_id}")
    print(f"{'='*60}")
    
    async with httpx.AsyncClient(timeout=10) as client:
        # Test endpoint 1
        response = await client.get(
            f"{BASE_URL}/sensors/{sensor_id}",
            headers=headers
        )
        print(f"STATUS: {response.status_code}")
        print(f"RESPONSE:")
        print(json.dumps(response.json(), indent=2, ensure_ascii=False))
        
        # Test endpoint 2: /v3/sensors
        print(f"\n--- /v3/sensors with id filter ---")
        response2 = await client.get(
            f"{BASE_URL}/sensors",
            params={"id": sensor_id},
            headers=headers
        )
        print(f"STATUS: {response2.status_code}")
        if response2.status_code == 200:
            data = response2.json()
            print(f"FOUND: {len(data.get('results', []))}")
            if data.get('results'):
                print(json.dumps(data['results'][0], indent=2, ensure_ascii=False))


async def main():
    # Test với sensor IDs từ output trước
    sensor_ids = [13502156, 13502155, 13502154, 13502152, 13502164, 13502149]
    
    for sid in sensor_ids[:2]:  # Test 2 cái đầu
        await test_sensor(sid)


if __name__ == "__main__":
    asyncio.run(main())