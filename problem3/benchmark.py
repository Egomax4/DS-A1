import glob
import json
import os
import random
import subprocess
import sys
import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np

from testgen import generate_fast_graph

random.seed(42)


SIZES = [10, 100, 1000, 10_000, 100_000, 1_000_000]
PROCS = [1, 2, 4, 8]
RUNS = 5
OUTPUT_DIR = Path("./benchmark_output")
TARGET_SCRIPT = "profiled_components.py"
DATA_FILE = Path("temp_graph.data")


def run_benchmark():
    OUTPUT_DIR.mkdir(exist_ok=True)

    results = {n: {p: [] for p in PROCS} for n in SIZES}

    print(
        f"{'Nodes':<10} {'Run':<4} {'Procs':<6} {'Time(s)':<10} {'Status':<10}"
    )
    print("-" * 50)

    for n in SIZES:
        k = max(1, n // 10)

        for r in range(1, RUNS + 1):
            with open(DATA_FILE, "w") as f:
                generate_fast_graph(n, k, file=Path(DATA_FILE))

            for p in PROCS:
                for f in glob.glob(str(OUTPUT_DIR / "profile_*.json")):
                    try:
                        os.remove(f)
                    except OSError:
                        pass

                cmd = [
                    "mpiexec",
                    "-n",
                    str(p),
                    sys.executable,
                    TARGET_SCRIPT,
                    str(DATA_FILE),
                ]

                log_file = OUTPUT_DIR / f"log_{n}_{r}_{p}.txt"
                t_start = time.perf_counter()

                try:
                    with open(log_file, "w") as log_f:
                        result = subprocess.run(
                            cmd,
                            stdout=log_f,
                            stderr=log_f,
                            timeout=600,
                        )

                    t_end = time.perf_counter()
                    duration = t_end - t_start

                    if result.returncode != 0:
                        print(
                            f"{n:<10} {r:<4} {p:<6} {'FAILED':<10} {result.returncode}"
                        )
                        continue

                    comm_time_sum = 0
                    comp_time_sum = 0
                    total_bytes = 0
                    iterations = 0

                    profiles = glob.glob(str(OUTPUT_DIR / "profile_*.json"))
                    if not profiles:
                        print(
                            f"{n:<10} {r:<4} {p:<6} {'NO_PROF':<10} {'ERROR'}"
                        )
                        continue

                    for prof_file in profiles:
                        with open(prof_file, "r") as pf:
                            data = json.load(pf)
                            comm_time_sum += data["comm_time"]
                            comp_time_sum += data["comp_time"]
                            total_bytes += data["msg_stats"]["sent_bytes"]
                            iterations = max(iterations, data["iterations"])

                    avg_comm = comm_time_sum / p
                    avg_comp = comp_time_sum / p

                    metric = {
                        "total_time": duration,
                        "avg_comm": avg_comm,
                        "avg_comp": avg_comp,
                        "total_bytes": total_bytes,
                        "iterations": iterations,
                    }

                    results[n][p].append(metric)
                    print(f"{n:<10} {r:<4} {p:<6} {duration:.4f} {'OK'}")

                except subprocess.TimeoutExpired:
                    print(f"{n:<10} {r:<4} {p:<6} {'>600s':<10} {'TIMEOUT'}")
                except Exception as e:
                    print(f"Error: {e}")

    final_stats = {}

    for n in SIZES:
        for p in PROCS:
            runs = results[n][p]
            if not runs:
                continue

            avg_run = {
                "total_time": np.mean([m["total_time"] for m in runs]),
                "avg_comm": np.mean([m["avg_comm"] for m in runs]),
                "avg_comp": np.mean([m["avg_comp"] for m in runs]),
                "total_bytes": np.mean([m["total_bytes"] for m in runs]),
                "iterations": np.mean([m["iterations"] for m in runs]),
                "time_std": np.std([m["total_time"] for m in runs]),
            }
            final_stats[f"{n}_{p}"] = avg_run

    with open(OUTPUT_DIR / "final_results.json", "w") as f:
        json.dump(final_stats, f, indent=2)

    generate_plots(final_stats)
    write_report(final_stats)

    if DATA_FILE.exists():
        DATA_FILE.unlink()


def generate_plots(results):
    parsed_keys = [list(map(int, k.split("_"))) for k in results.keys()]
    sizes = sorted(list(set(k[0] for k in parsed_keys)))
    procs = sorted(list(set(k[1] for k in parsed_keys)))

    plt.figure(figsize=(10, 6))
    for p in procs:
        times = []
        my_sizes = []
        for n in sizes:
            key = f"{n}_{p}"
            if key in results:
                my_sizes.append(n)
                times.append(results[key]["total_time"])
        plt.loglog(my_sizes, times, marker="o", label=f"Procs={p}")

    plt.xlabel("Graph Size (Nodes)")
    plt.ylabel("Execution Time (s)")
    plt.title("Execution Time vs Graph Size")
    plt.legend()
    plt.grid(True, which="both", ls="-")
    plt.savefig(OUTPUT_DIR / "plot_scalability.png")
    plt.close()

    valid_sizes = [n for n in sizes if f"{n}_1" in results]
    if valid_sizes:
        largest_size = valid_sizes[-1]
        plt.figure(figsize=(10, 6))
        speedups = []

        base_time = results[f"{largest_size}_1"]["total_time"]
        current_procs = []
        for p in procs:
            key = f"{largest_size}_{p}"
            if key in results:
                current_procs.append(p)
                s = base_time / results[key]["total_time"]
                speedups.append(s)

        plt.plot(current_procs, speedups, marker="s", label="Measured")
        plt.plot(current_procs, current_procs, "k--", label="Ideal Linear")
        plt.xlabel("Number of Processes")
        plt.ylabel("Speedup")
        plt.title(f"Strong Scaling Speedup (N={largest_size})")
        plt.legend()
        plt.grid(True)
        plt.savefig(OUTPUT_DIR / "plot_speedup.png")
        plt.close()

    plt.figure(figsize=(10, 6))
    for p in procs:
        if p == 1:
            continue
        ratios = []
        my_sizes = []
        for n in sizes:
            key = f"{n}_{p}"
            if key in results:
                m = results[key]
                ratio = m["avg_comm"] / m["total_time"]
                ratios.append(ratio)
                my_sizes.append(n)
        plt.semilogx(my_sizes, ratios, marker="^", label=f"Procs={p}")

    plt.xlabel("Graph Size (Nodes)")
    plt.ylabel("Communication / Total Time Ratio")
    plt.title("Communication Overhead Analysis")
    plt.legend()
    plt.grid(True)
    plt.savefig(OUTPUT_DIR / "plot_comm_ratio.png")
    plt.close()


def write_report(results):
    parsed_keys = [list(map(int, k.split("_"))) for k in results.keys()]
    sizes = sorted(list(set(k[0] for k in parsed_keys)))
    procs = sorted(list(set(k[1] for k in parsed_keys)))

    with open(OUTPUT_DIR / "analysis_report.txt", "w") as f:
        f.write("BENCHMARK ANALYSIS REPORT\n")
        f.write("=========================\n\n")

        f.write("1. PARALLEL EFFICIENCY\n")
        valid_sizes = [n for n in sizes if f"{n}_1" in results]
        if valid_sizes:
            largest = valid_sizes[-1]
            t1 = results[f"{largest}_1"]["total_time"]
            f.write(f"Reference Time (N={largest}, P=1): {t1:.4f}s\n")

            for p in procs:
                if p == 1:
                    continue
                key = f"{largest}_{p}"
                if key in results:
                    tp = results[key]["total_time"]
                    speedup = t1 / tp
                    eff = speedup / p
                    f.write(
                        f"Procs={p}: Speedup={speedup:.2f}x, Efficiency={eff:.2%}\n"
                    )

        f.write("\n2. COMPUTATIONAL COMPLEXITY\n")
        f.write("Algorithm: Label Propagation\n")
        f.write("Theoretical Complexity: O(k * (n + m)) per iteration.\n")
        f.write("Measured Scaling (P=1):\n")

        for n in sizes:
            key = f"{n}_1"
            if key in results:
                t = results[key]["total_time"]
                iters = results[key]["iterations"]
                f.write(
                    f"N={n}: {t:.4f}s ({iters:.1f} avg iters) -> {t / n:.2e} sec/node\n"
                )

        f.write("\n3. COMMUNICATION BOTTLENECKS\n")
        if valid_sizes:
            largest = valid_sizes[-1]
            for p in procs:
                if p == 1:
                    continue
                key = f"{largest}_{p}"
                if key in results:
                    comm = results[key]["avg_comm"]
                    total = results[key]["total_time"]
                    f.write(
                        f"Procs={p}, N={largest}: Comm overhead is {comm / total:.1%}\n"
                    )


if __name__ == "__main__":
    run_benchmark()
