from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

RESULTS = Path("results")

if (RESULTS / "fog_metrics.csv").exists():
    m = pd.read_csv(RESULTS / "fog_metrics.csv")
    plt.figure(figsize=(11, 5))
    plt.plot(m["timestamp"] - m["timestamp"].min(), m["edge_cpu"], label="Edge CPU")
    plt.plot(m["timestamp"] - m["timestamp"].min(), m["fog_cpu"], label="Fog CPU")
    plt.axhline(90, linestyle="--", label="Failover threshold")
    plt.axhline(50, linestyle=":", label="Recovery threshold")
    plt.xlabel("Time (seconds)")
    plt.ylabel("CPU utilisation (%)")
    plt.title("Edge vs Fog CPU and Failover Thresholds")
    plt.legend()
    plt.tight_layout()
    plt.savefig(RESULTS / "cpu_routing.png", dpi=160)
    plt.close()

    counts = pd.Series({
        "Edge": int(m["routed_edge"].max()),
        "Fog": int(m["routed_fog"].max()),
        "Cloud": int(m["routed_cloud"].max()),
    })
    plt.figure(figsize=(8, 5))
    counts.plot(kind="bar")
    plt.xlabel("Processing tier")
    plt.ylabel("Tasks")
    plt.title("Workload Routing by Tier")
    plt.tight_layout()
    plt.savefig(RESULTS / "task_distribution.png", dpi=160)
    plt.close()

if (RESULTS / "stress_results.csv").exists():
    df = pd.read_csv(RESULTS / "stress_results.csv")
    slo = 5.0
    df["task_number"] = range(1, len(df)+1)
    df["slo_violation"] = df["latency_seconds"] > slo

    plt.figure(figsize=(11, 5))
    plt.plot(df["task_number"], df["latency_seconds"])
    plt.axhline(slo, linestyle="--", label="5s SLO")
    plt.xlabel("Task")
    plt.ylabel("Latency (seconds)")
    plt.title("Task Latency vs SLO")
    plt.legend()
    plt.tight_layout()
    plt.savefig(RESULTS / "latency_slo.png", dpi=160)
    plt.close()

    counts = df["destination"].value_counts()
    plt.figure(figsize=(8, 5))
    counts.plot(kind="bar")
    plt.xlabel("Processing tier")
    plt.ylabel("Tasks")
    plt.title("Task Distribution by Processing Tier")
    plt.tight_layout()
    plt.savefig(RESULTS / "stress_task_distribution.png", dpi=160)
    plt.close()

    print("Total:", len(df))
    print("SLO violations:", int(df["slo_violation"].sum()))
    print("SLO compliance:", f"{1-df['slo_violation'].mean():.2%}")

print("Analysis complete. See results/")
