import argparse
import concurrent.futures
import csv
import time
import uuid
from pathlib import Path

import requests


def submit(base, i):
    payload = {
        "fog_id": "stress-test",
        "processed_by": "fog",
        "readings": [{
            "sensor_id": f"stress-{i}",
            "sensor_type": ["temperature","humidity","pressure","light","vibration"][i % 5],
            "value": 20 + (i % 30) / 10,
            "unit": "test",
            "timestamp": time.time(),
        }],
    }
    start = time.time()
    try:
        r = requests.post(f"{base}/api/v1/sensor-data", json=payload, timeout=5)
        latency = time.time() - start
        return [i, "cloud-api", latency, "success" if r.ok else "failure"]
    except Exception as exc:
        return [i, "cloud-api", time.time()-start, f"timeout/error:{exc}"]

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--tasks", type=int, default=1000)
    ap.add_argument("--workers", type=int, default=40)
    args = ap.parse_args()

    out = Path("results")
    out.mkdir(exist_ok=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as ex:
        rows = list(ex.map(lambda i: submit(args.url, i), range(args.tasks)))

    path = out / "stress_results.csv"
    with path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["task_id","destination","latency_seconds","status"])
        w.writerows(rows)

    print(f"Wrote {path}")
    ok = sum(1 for r in rows if r[3] == "success")
    print(f"Success: {ok}/{args.tasks} = {ok/args.tasks:.2%}")

if __name__ == "__main__":
    main()
