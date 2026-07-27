import argparse
import csv
import time
from pathlib import Path
import requests

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8102")
    ap.add_argument("--seconds", type=int, default=120)
    args = ap.parse_args()

    out = Path("results")
    out.mkdir(exist_ok=True)
    with (out / "fog_metrics.csv").open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["timestamp","edge_cpu","fog_cpu","state","routed_edge","routed_fog","routed_cloud"])
        end = time.time() + args.seconds
        while time.time() < end:
            try:
                x = requests.get(f"{args.url}/metrics", timeout=2).json()
                w.writerow([
                    time.time(), x["edge_cpu"], x["fog_cpu"], x["state"],
                    x["routed_edge"], x["routed_fog"], x["routed_cloud"]
                ])
                print(x["state"], "edge=", x["edge_cpu"], "fog=", x["fog_cpu"])
            except Exception as e:
                print("metrics error:", e)
            time.sleep(1)

if __name__ == "__main__":
    main()
