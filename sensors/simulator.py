import asyncio
import os
import random
import time
import uuid

import httpx
import yaml

CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yaml")
FOG_URL = os.getenv("FOG_URL", "http://localhost:8102")
MULTIPLIER = max(0.1, float(os.getenv("SENSOR_MULTIPLIER", "1")))

with open(CONFIG_PATH, "r") as f:
    cfg = yaml.safe_load(f)

RANGES = {
    "temperature": (18, 35),
    "humidity": (35, 85),
    "pressure": (990, 1035),
    "light": (50, 1200),
    "vibration": (0.05, 3.0),
}

async def emit(sensor_type, frequency):
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            lo, hi = RANGES[sensor_type]
            reading = {
                "sensor_id": f"{sensor_type.upper()}-001",
                "sensor_type": sensor_type,
                "value": round(random.uniform(lo, hi), 3),
                "unit": cfg["sensors"][sensor_type]["unit"],
                "timestamp": time.time(),
            }
            try:
                await client.post(f"{FOG_URL}/ingest", json=reading)
            except Exception as exc:
                print(f"[SENSOR] send failed: {exc}")
            await asyncio.sleep(max(0.01, frequency / MULTIPLIER))

async def main():
    await asyncio.gather(*[
        emit(name, float(item["frequency_seconds"]))
        for name, item in cfg["sensors"].items()
    ])

asyncio.run(main())
