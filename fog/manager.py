import asyncio
import os
import statistics
import time
import uuid
from collections import defaultdict, deque

import httpx
import psutil
import yaml
from fastapi import FastAPI
from pydantic import BaseModel

EDGE_URL = os.getenv("EDGE_URL", "http://localhost:8101")
BACKEND_URL = os.getenv("BACKEND_URL", "http://localhost:8000")
CONFIG_PATH = os.getenv("CONFIG_PATH", "/app/config.yaml")

with open(CONFIG_PATH, "r") as f:
    CONFIG = yaml.safe_load(f)

DISPATCH_INTERVAL = float(CONFIG["fog"]["dispatch_interval_seconds"])
BATCH_SIZE = int(CONFIG["fog"]["batch_size"])
EDGE_FAIL = float(CONFIG["fog"]["edge_failover_cpu_percent"])
EDGE_RECOVER = float(CONFIG["fog"]["edge_recovery_cpu_percent"])
FOG_FAIL = float(CONFIG["fog"]["fog_failover_cpu_percent"])
FOG_RECOVER = float(CONFIG["fog"]["fog_recovery_cpu_percent"])

app = FastAPI(title="Virtual Fog SRE Manager")

state = "EDGE_NORMAL"
buffer = deque()
stats = {
    "edge_cpu": 0,
    "fog_cpu": 0,
    "cloud_active": False,
    "state": state,
    "routed_edge": 0,
    "routed_fog": 0,
    "routed_cloud": 0,
    "dispatched_batches": 0,
}
history = deque(maxlen=10000)

class Reading(BaseModel):
    sensor_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: float

async def edge_metrics():
    async with httpx.AsyncClient(timeout=1.5) as client:
        r = await client.get(f"{EDGE_URL}/metrics")
        r.raise_for_status()
        return r.json()

async def route_to_edge(reading: dict):
    async with httpx.AsyncClient(timeout=6) as client:
        r = await client.post(f"{EDGE_URL}/process", json={
            "task_id": str(uuid.uuid4()),
            **reading,
            "timeout_seconds": CONFIG["load"]["task_timeout_seconds"],
        })
        r.raise_for_status()
        stats["routed_edge"] += 1
        return r.json()

def fog_local_process(reading: dict):
    # Lightweight fog-side processing. The Fog is the control plane,
    # but it can also execute overflow work.
    stats["routed_fog"] += 1
    return {
        **reading,
        "processed_at": time.time(),
        "processed_by": "fog",
    }

async def send_batch(readings):
    if not readings:
        return

    print("\n===== Sending batch =====")
    for r in readings:
        print(r["sensor_type"], r["processed_by"])
    print("=========================\n")

    payload = {
        "fog_id": "fog-001",
        #"processed_by": "fog",
        "readings": readings,
        "dispatch_timestamp": time.time(),
    }
    async with httpx.AsyncClient(timeout=8) as client:
        r = await client.post(f"{BACKEND_URL}/api/v1/sensor-data", json=payload)
        r.raise_for_status()
    stats["dispatched_batches"] += 1

async def manager_loop():
    global state
    while True:
        try:
            m = await edge_metrics()
            edge_cpu = float(m["cpu_percent"])
        except Exception:
            edge_cpu = 100.0

        # Fog's own process is used as a simple local control-plane metric.
        fog_cpu = psutil.cpu_percent(interval=0.05)
        stats["edge_cpu"] = edge_cpu
        stats["fog_cpu"] = fog_cpu

        if state == "EDGE_NORMAL" and edge_cpu >= EDGE_FAIL:
            state = "FOG_ACTIVE"
            print(f"[FOG] EDGE_NORMAL -> FOG_ACTIVE; edge_cpu={edge_cpu:.1f}%")

        elif state == "FOG_ACTIVE" and fog_cpu >= FOG_FAIL:
            state = "CLOUD_ACTIVE"
            print(f"[FOG] FOG_ACTIVE -> CLOUD_ACTIVE; fog_cpu={fog_cpu:.1f}%")

        elif state == "CLOUD_ACTIVE" and fog_cpu <= FOG_RECOVER:
            state = "FOG_ACTIVE"
            print(f"[FOG] CLOUD_ACTIVE -> FOG_ACTIVE; fog_cpu={fog_cpu:.1f}%")

        elif state == "FOG_ACTIVE" and edge_cpu <= EDGE_RECOVER:
            state = "EDGE_NORMAL"
            print(f"[FOG] FOG_ACTIVE -> EDGE_NORMAL; edge_cpu={edge_cpu:.1f}%")

        stats["state"] = state
        stats["cloud_active"] = state == "CLOUD_ACTIVE"
        history.append({
            "timestamp": time.time(),
            "edge_cpu": edge_cpu,
            "fog_cpu": fog_cpu,
            "state": state,
        })
        await asyncio.sleep(1)

@app.on_event("startup")
async def startup():
    asyncio.create_task(manager_loop())

@app.get("/health")
def health():
    return {"status": "healthy", "node": "fog", "state": state}

@app.get("/metrics")
def metrics():
    return {**stats, "history": list(history)[-300:]}

@app.post("/ingest")
async def ingest(reading: Reading):
    data = reading.model_dump()

    # The Fog receives every reading and makes the routing decision.
    if state == "EDGE_NORMAL":
        try:
            result = await route_to_edge(data)
            processed = { **data,**result}
        except Exception as exc:
            processed = {**data, "processed_by": "fog", "fallback_reason": str(exc)}
            processed = fog_local_process(processed)
    elif state == "FOG_ACTIVE":
        processed = fog_local_process(data)
    else:
        # Cloud-active mode sends the task to the backend. The backend's
        # SQS/Lambda layer is the scalable cloud processing tier.
        processed = {**data, "processed_by": "cloud"}
        stats["routed_cloud"] += 1

    buffer.append(processed)
    if len(buffer) >= BATCH_SIZE:
        batch = [buffer.popleft() for _ in range(min(BATCH_SIZE, len(buffer)))]
        await send_batch(batch)

    return {"accepted": True, "state": state, "processed_by": processed["processed_by"]}

@app.post("/dispatch")
async def dispatch():
    batch = []
    while buffer and len(batch) < BATCH_SIZE:
        batch.append(buffer.popleft())
    await send_batch(batch)
    return {"sent": len(batch)}
