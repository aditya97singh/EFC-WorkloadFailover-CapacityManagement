import hashlib
import os
import time
from collections import deque
from pathlib import Path

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

_cpu_prev_usage = None
_cpu_prev_time = None


def read_cgroup_cpu():
    """
    Return CPU utilisation relative to this container's CPU quota.
    Works with cgroup v2 and falls back to cgroup v1.
    """
    global _cpu_prev_usage, _cpu_prev_time

    now = time.monotonic()

    # cgroup v2
    cpu_stat = Path("/sys/fs/cgroup/cpu.stat")
    cpu_max = Path("/sys/fs/cgroup/cpu.max")

    if cpu_stat.exists() and cpu_max.exists():
        stats = {}
        for line in cpu_stat.read_text().splitlines():
            key, value = line.split()
            stats[key] = int(value)

        usage_usec = stats.get("usage_usec", 0)

        max_parts = cpu_max.read_text().strip().split()

        if max_parts[0] == "max":
            quota_cpus = os.cpu_count() or 1
        else:
            quota = int(max_parts[0])
            period = int(max_parts[1])
            quota_cpus = quota / period

    else:
        # cgroup v1 fallback
        usage_path = Path("/sys/fs/cgroup/cpuacct/cpuacct.usage")
        quota_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_quota_us")
        period_path = Path("/sys/fs/cgroup/cpu/cpu.cfs_period_us")

        if not (usage_path.exists() and quota_path.exists() and period_path.exists()):
            return psutil.cpu_percent(interval=0.05)

        usage_usec = int(usage_path.read_text()) / 1000
        quota = int(quota_path.read_text())
        period = int(period_path.read_text())

        if quota <= 0:
            quota_cpus = os.cpu_count() or 1
        else:
            quota_cpus = quota / period

    if _cpu_prev_usage is None:
        _cpu_prev_usage = usage_usec
        _cpu_prev_time = now
        return 0.0

    delta_usage = usage_usec - _cpu_prev_usage
    delta_time_usec = (now - _cpu_prev_time) * 1_000_000

    _cpu_prev_usage = usage_usec
    _cpu_prev_time = now

    if delta_time_usec <= 0:
        return 0.0

    # CPU utilisation relative to container quota
    cpu_percent = (
        (delta_usage / delta_time_usec)
        / max(quota_cpus, 0.001)
        * 100
    )

    return min(100.0, max(0.0, cpu_percent))    

@app.get("/health")
def health():
    return {"status": "healthy", "node": "edge"}

@app.get("/metrics")
def metrics():
    return {
        "node": "edge",
        "cpu_percent": round(read_cgroup_cpu(), 2),
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
