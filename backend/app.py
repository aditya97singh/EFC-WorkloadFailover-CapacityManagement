import json
import os
import sqlite3
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

try:
    import boto3
except ImportError:
    boto3 = None

app = FastAPI(title="Edge-Fog-Cloud Sensor Backend", version="1.0.0")

STORAGE_MODE = os.getenv("STORAGE_MODE", "sqlite").lower()
DATABASE_PATH = os.getenv("DATABASE_PATH", "/data/sensors.db")
SQS_ENABLED = os.getenv("SQS_ENABLED", "false").lower() == "true"
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL", "")
DDB_TABLE = os.getenv("DDB_TABLE", "edge-fog-sensor-data")

class Reading(BaseModel):
    sensor_id: str
    sensor_type: str
    value: float
    unit: str
    timestamp: float
    processed_by: str | None = None
    edge_node: str | None = None
    latency: float | None = None
    status: str | None = None

class SensorBatch(BaseModel):
    fog_id: str = "fog-001"
    readings: list[Reading] = Field(default_factory=list)
    #processed_by: str = "fog". #Due to overwriting Edge node readings
    #readings: list[dict[str, Any]] = Field(default_factory=list).   #to resolve reading, have same consistent structure
    dispatch_timestamp: float = Field(default_factory=time.time)

def init_sqlite():
    os.makedirs(os.path.dirname(DATABASE_PATH) or ".", exist_ok=True)
    with sqlite3.connect(DATABASE_PATH) as con:
        con.execute("""
        CREATE TABLE IF NOT EXISTS readings (
          id TEXT PRIMARY KEY,
          sensor_type TEXT NOT NULL,
          sensor_id TEXT,
          value REAL NOT NULL,
          unit TEXT,
          timestamp REAL NOT NULL,
          processed_by TEXT,
          fog_id TEXT
        )
        """)
        con.commit()

if STORAGE_MODE == "sqlite":
    init_sqlite()

def store_local(batch: SensorBatch):
    with sqlite3.connect(DATABASE_PATH) as con:
        for r in batch.readings:
            con.execute(
                """INSERT INTO readings
                   (id, sensor_type, sensor_id, value, unit, timestamp, processed_by, fog_id)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    str(uuid.uuid4()),
                    r.sensor_type,
                    r.sensor_id,
                    r.value,
                    r.unit,
                    r.timestamp,
                    r.processed_by,
                    batch.fog_id,
                ),
            )
        con.commit()

def store_dynamodb(batch: SensorBatch):
    if boto3 is None:
        raise RuntimeError("boto3 is required for DynamoDB mode")
    table = boto3.resource("dynamodb").Table(DDB_TABLE)
    with table.batch_writer() as writer:
        for r in batch.readings:
            writer.put_item(Item={
                "id": str(uuid.uuid4()),
                "sensor_type": r.sensor_type,
                "sensor_id": r.sensor_id,
                "value": r.value,
                "unit": r.unit,
                "timestamp": r.timestamp,
                "processed_by": r.processed_by,
                "fog_id": batch.fog_id,
            })

def store(batch: SensorBatch):
    if STORAGE_MODE == "dynamodb":
        store_dynamodb(batch)
    else:
        store_local(batch)

def queue_batch(batch: SensorBatch):
    if not SQS_ENABLED:
        return False
    if boto3 is None or not SQS_QUEUE_URL:
        raise RuntimeError("SQS is enabled but boto3/SQS_QUEUE_URL is missing")
    sqs = boto3.client("sqs")
    sqs.send_message(QueueUrl=SQS_QUEUE_URL, MessageBody=batch.model_dump_json())
    return True

@app.get("/health")
def health():
    return {"status": "healthy", "storage": STORAGE_MODE, "sqs_enabled": SQS_ENABLED}

@app.get("/", response_class=HTMLResponse)
def dashboard():
    return HTMLResponse(DASHBOARD_HTML)

@app.post("/api/v1/sensor-data")
def receive_sensor_data(batch: SensorBatch):
    if not batch.readings:
        raise HTTPException(status_code=400, detail="readings must not be empty")
    if SQS_ENABLED:
        queue_batch(batch)
        return {"status": "accepted", "queued": True, "count": len(batch.readings)}
    store(batch)
    return {"status": "accepted", "queued": False, "count": len(batch.readings)}

def query_local(sensor_type: str | None = None, limit: int = 200):
    sql = """SELECT sensor_type, sensor_id, value, unit, timestamp, processed_by, fog_id
             FROM readings"""
    params = []
    if sensor_type:
        sql += " WHERE sensor_type = ?"
        params.append(sensor_type)
    sql += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    with sqlite3.connect(DATABASE_PATH) as con:
        con.row_factory = sqlite3.Row
        return [dict(r) for r in con.execute(sql, params).fetchall()]

def query_dynamodb(sensor_type: str | None = None, limit: int = 200):
    if boto3 is None:
        raise RuntimeError("boto3 is required for DynamoDB mode")
    table = boto3.resource("dynamodb").Table(DDB_TABLE)
    items = table.scan(Limit=min(limit, 1000)).get("Items", [])
    if sensor_type:
        items = [x for x in items if x.get("sensor_type") == sensor_type]
    items.sort(key=lambda x: float(x.get("timestamp", 0)), reverse=True)
    return items[:limit]

@app.get("/api/v1/sensors")
def sensors(sensor_type: str | None = None, limit: int = 200):
    limit = max(1, min(limit, 1000))
    return {"items": query_dynamodb(sensor_type, limit) if STORAGE_MODE == "dynamodb"
            else query_local(sensor_type, limit)}

@app.get("/api/v1/sensors/{sensor_type}")
def sensor_type(sensor_type: str, limit: int = 200):
    return sensors(sensor_type=sensor_type, limit=limit)

@app.get("/api/v1/system/status")
def system_status():
    items = sensors(limit=50)["items"]
    counts = {}
    for item in items:
        tier = item.get("processed_by", "unknown")
        counts[tier] = counts.get(tier, 0) + 1
    return {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "backend": "FastAPI",
        "storage": STORAGE_MODE,
        "recent_processing_counts": counts,
    }

DASHBOARD_HTML = r"""
<!doctype html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Edge → Fog → Cloud Dashboard</title>
<style>
body{font-family:system-ui;margin:0;background:#f5f7fb;color:#182230}
header{padding:22px 5%;background:#172033;color:white}
main{max-width:1200px;margin:25px auto;padding:0 20px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(190px,1fr));gap:15px}
.card{background:white;border-radius:14px;padding:18px;box-shadow:0 2px 12px #0001}
.value{font-size:28px;font-weight:700}
canvas{width:100%;height:260px}
.small{color:#687386;font-size:13px}
</style>
</head>
<body>
<header><h1>Edge → Fog → Cloud Sensor Dashboard</h1><div>FastAPI backend · live polling</div></header>
<main>
<div id="cards" class="grid"></div>
<div class="card" style="margin-top:20px">
<h2>Recent readings</h2><canvas id="chart" width="900" height="260"></canvas>
</div>
<div class="card" style="margin-top:20px">
<h2>Processing tiers</h2><pre id="status">Loading...</pre>
</div>
</main>
<script>
const sensors=["temperature","humidity","pressure","light","vibration"];
async function refresh(){
  const all=await (await fetch("/api/v1/sensors?limit=300")).json();
  const groups={};
  all.items.forEach(x=>(groups[x.sensor_type]??=[]).push(x));
  document.getElementById("cards").innerHTML=sensors.map(s=>{
    const x=(groups[s]||[])[0];
    return `<div class="card"><div class="small">${s}</div>
      <div class="value">${x?Number(x.value).toFixed(2):"—"}</div>
      <div class="small">${x?.unit||""} · ${x?.processed_by||"waiting"}</div></div>`;
  }).join("");
  const st=await (await fetch("/api/v1/system/status")).json();
  document.getElementById("status").textContent=JSON.stringify(st,null,2);
  draw(groups.temperature||[]);
}
function draw(rows){
 const c=document.getElementById("chart"),ctx=c.getContext("2d");
 ctx.clearRect(0,0,c.width,c.height);
 if(!rows.length)return;
 const vals=rows.slice(0,80).reverse().map(x=>Number(x.value));
 const min=Math.min(...vals),max=Math.max(...vals)||1;
 ctx.beginPath();
 vals.forEach((v,i)=>{
   const x=40+i*(c.width-60)/Math.max(1,vals.length-1);
   const y=220-(v-min)/(max-min||1)*180;
   i?ctx.lineTo(x,y):ctx.moveTo(x,y);
 });
 ctx.stroke();
}
refresh();setInterval(refresh,2000);
</script>
</body>
</html>
"""
