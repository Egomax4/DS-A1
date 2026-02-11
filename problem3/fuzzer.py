import sys
import time
from pathlib import Path
from random import randint, seed
from subprocess import run

from testgen import generate_fast_graph


def run_tests(num_tests: int, target_script: str = "components.py"):
    passed = 0
    data_file = Path("./temp.data")

    for i in range(1, num_tests + 1):
        this_seed = int(time.monotonic_ns())
        seed(this_seed)
        num_nodes = randint(900_000, 1_000_000)
        num_components = randint(1, num_nodes)
        num_procs = randint(1, 8)

        print(
            f"Test {i}/{num_tests}: Seed={this_seed}, Nodes={num_nodes}, K={num_components}, Procs={num_procs}...",
            end=" ",
            flush=True,
        )

        with open(data_file, mode="w") as f:
            original_stdout = sys.stdout
            sys.stdout = f
            try:
                generate_fast_graph(num_nodes, num_components)
            finally:
                sys.stdout = original_stdout

        cmd = f"mpiexec -n {num_procs} {sys.executable} {target_script} {data_file}"

        tic = time.monotonic_ns()
        result = run(cmd, shell=True, capture_output=True, text=True)
        toc = time.monotonic_ns()

        if result.returncode != 0:
            print("CRASHED")
            print(f"Stdout: {result.stdout}")
            print(f"Stderr: {result.stderr}")
            continue

        try:
            output_k = int(result.stdout.strip())

            if output_k == num_components:
                print(f"PASSED ({(toc - tic) / 1e9:.4f} seconds)")
                passed += 1
            else:
                print(f"FAILED (Expected {num_components}, Got {output_k})")

        except ValueError:
            print(
                f"INVALID OUTPUT (Expected int, Got: '{result.stdout.strip()}')"
            )

    print(f"\nSummary: {passed}/{num_tests} tests passed.")

    if data_file.exists():
        data_file.unlink()


def run_single(temp_seed: int, target_script: str = "components.py"):
    seed(temp_seed)
    num_nodes = randint(900_000, 1_000_000)
    num_components = randint(1, num_nodes)
    num_procs = randint(1, 8)

    print(
        f"Single Test: Seed={temp_seed}, Nodes={num_nodes}, K={num_components}, Procs={num_procs}...",
        end=" ",
        flush=True,
    )

    data_file = Path("./temp.data")
    with open(data_file, mode="w") as f:
        original_stdout = sys.stdout
        sys.stdout = f
        try:
            generate_fast_graph(num_nodes, num_components)
        finally:
            sys.stdout = original_stdout

    cmd = (
        f"mpiexec -n {num_procs} {sys.executable} {target_script} {data_file}"
    )

    tic = time.monotonic_ns()
    result = run(cmd, shell=True, text=True, capture_output=True)
    toc = time.monotonic_ns()

    if result.returncode != 0:
        print("CRASHED")
        print(f"Stdout: {result.stdout}")
        print(f"Stderr: {result.stderr}")
        return

    try:
        output_k = int(result.stdout.strip())

        if output_k == num_components:
            print(f"PASSED ({(toc - tic) / 1e9:.4f} seconds)")
        else:
            print(f"FAILED (Expected {num_components}, Got {output_k})")

    except ValueError:
        print(f"INVALID OUTPUT (Expected int, Got: '{result.stdout.strip()}')")


if __name__ == "__main__":
    if len(sys.argv) >= 2:
        n_tests = 1
        try:
            n_tests = int(sys.argv[1])
        except ValueError:
            print(
                "Error: First argument must be an integer (number of tests)."
            )
        script = sys.argv[2] if len(sys.argv) > 2 else "components.py"
        if n_tests > 10000:
            run_single(n_tests, script)
        else:
            run_tests(n_tests, script)
    else:
        print("Usage: python run_tests.py <num_tests> [target_script.py]")
