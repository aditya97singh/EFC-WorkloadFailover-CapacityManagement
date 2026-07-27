import hashlib
import os
import time
from collections import deque

import psutil
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI(title="Virtual Edge Worker")
CPU_WORK_ITERATIONS = int(os.getenv("CPU_WORK_ITERATIONS", "180000"))
recent = deque(maxlen=1000)
processed = 0
'''
class Task(BaseModel):
    task_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: float
    timeout_seconds: float = 5
'''
class Task(BaseModel):
    task_id: str
    sensor_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: float
    timeout_seconds: float = 5

def cpu_work(iterations: int):
    data = b"edge-fog-cloud"
    digest = data
    for _ in range(iterations):
        digest = hashlib.sha256(digest).digest()
    return digest.hex()

@app.get("/health")
def health():
    return {"status": "healthy", "node": "edge"}

@app.get("/metrics")
def metrics():
    return {
        "node": "edge",
        "cpu_percent": psutil.cpu_percent(interval=0.05),
        "memory_percent": psutil.virtual_memory().percent,
        "processed": processed,
    }

@app.post("/process")
def process(task: Task):
    global processed
    started = time.time()
    cpu_work(CPU_WORK_ITERATIONS)
    elapsed = time.time() - started
    processed += 1
    recent.append({
        "task_id": task.task_id,
        "latency": elapsed,
        "completed_at": time.time(),
        "node": "edge",
    })
    return {
    "sensor_id": task.sensor_id,
    "sensor_type": task.sensor_type,
    "value": task.value,
    "unit": task.unit,
    "timestamp": task.timestamp,
    "processed_by": "edge",
    "edge_node": "edge-1",
    "latency": elapsed,
    "status": "completed"
}
