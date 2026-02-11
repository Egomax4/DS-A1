import csv
import statistics
import sys
import time
from pathlib import Path
from random import randint, seed
from subprocess import run

from testgen import generate_fast_graph


def benchmark_suite(
    target_script: str = "components.py",
    output_csv: str = "benchmark_results.csv",
):
    SEEDS = [42, 12345, 999, 7777, 8675309]
    PROC_COUNTS = [1, 2, 4, 8]
    REPEATS = 10
    DATA_FILE = Path("./temp_benchmark.data")

    headers = [
        "Seed",
        "Nodes",
        "Expected_K",
        "Procs",
        "Avg_Time_s",
        "StdDev_s",
        "Min_Time_s",
        "Max_Time_s",
    ] + [f"Run_{i + 1}_s" for i in range(REPEATS)]

    results = []

    print("Running benchmarks...")
    print(f"Target: {target_script}")
    print(f"Output: {output_csv}")
    print("-" * 60)

    try:
        for seed_idx, current_seed in enumerate(SEEDS):
            seed(current_seed)
            num_nodes = randint(10_000, 1_000_000)
            num_components = randint(1, num_nodes)

            print(
                f"\n[Seed {current_seed}] Generating Graph: {num_nodes} nodes, {num_components} components."
            )

            with open(DATA_FILE, mode="w") as f:
                original_stdout = sys.stdout
                sys.stdout = f
                try:
                    generate_fast_graph(num_nodes, num_components)
                finally:
                    sys.stdout = original_stdout

            for num_procs in PROC_COUNTS:
                print(
                    f"  > Testing with {num_procs} processes (x{REPEATS} runs)...",
                    end=" ",
                    flush=True,
                )

                run_times = []

                for r in range(REPEATS):
                    cmd = f"mpiexec -n {num_procs} {sys.executable} {target_script} {DATA_FILE}"

                    tic = time.monotonic_ns()
                    result = run(
                        cmd, shell=True, capture_output=True, text=True
                    )
                    toc = time.monotonic_ns()

                    duration = (toc - tic) / 1e9
                    run_times.append(duration)

                    if r == 0:
                        try:
                            output_k = int(result.stdout.strip())
                            if output_k != num_components:
                                print(
                                    f"[Warning: Output mismatch! Exp: {num_components}, Got: {output_k}]",
                                    end=" ",
                                )
                        except ValueError:
                            print("[Error: Invalid Output]", end=" ")

                avg_time = statistics.mean(run_times)
                std_dev = (
                    statistics.stdev(run_times) if len(run_times) > 1 else 0.0
                )
                min_time = min(run_times)
                max_time = max(run_times)

                print(f"Avg: {avg_time:.4f}s")

                row = [
                    current_seed,
                    num_nodes,
                    num_components,
                    num_procs,
                    round(avg_time, 6),
                    round(std_dev, 6),
                    round(min_time, 6),
                    round(max_time, 6),
                ] + [round(t, 6) for t in run_times]

                results.append(row)

    finally:
        if DATA_FILE.exists():
            DATA_FILE.unlink()

    try:
        with open(output_csv, mode="w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(results)
        print("-" * 60)
        print(f"Benchmark complete. Data saved to {output_csv}")
    except IOError as e:
        print(f"\nError saving CSV file: {e}")


if __name__ == "__main__":
    target = sys.argv[1] if len(sys.argv) > 1 else "components.py"
    benchmark_suite(target)
