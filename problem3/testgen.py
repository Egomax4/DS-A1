import random
import sys
from pathlib import Path
from typing import Optional


def generate_fast_graph(
    n, k, min_weight=1, max_weight=10, file: Optional[Path] = None
):
    if k > n:
        raise ValueError("k cannot be greater than n")

    nodes = list(range(n))
    random.shuffle(nodes)

    components = [[] for _ in range(k)]

    for i in range(k):
        components[i].append(nodes[i])

    for i in range(k, n):
        idx = random.randint(0, k - 1)
        components[idx].append(nodes[i])

    edges = []
    existing_edges = set()

    for comp in components:
        comp_size = len(comp)
        if comp_size < 2:
            continue

        for i in range(comp_size - 1):
            u, v = comp[i], comp[i + 1]
            w = random.randint(min_weight, max_weight)

            edges.append((u, v, w))

            if u < v:
                existing_edges.add((u, v))
            else:
                existing_edges.add((v, u))
        extra_edges_count = int(comp_size * 0.5)

        for _ in range(extra_edges_count):
            u = random.choice(comp)
            v = random.choice(comp)

            if u == v:
                continue

            pair = (u, v) if u < v else (v, u)

            if pair not in existing_edges:
                w = random.randint(min_weight, max_weight)
                edges.append((u, v, w))
                existing_edges.add(pair)

    if file is None:
        print(f"{n} {len(edges)}")
        for u, v, w in edges:
            print(f"{u} {v} {w}")
    else:
        with file.open("w") as f:
            f.write(f"{n} {len(edges)}\n")
            for u, v, w in edges:
                f.write(f"{u} {v} {w}\n")


if __name__ == "__main__":
    if len(sys.argv) >= 3:
        try:
            n_input = int(sys.argv[1])
            k_input = int(sys.argv[2])
            generate_fast_graph(n_input, k_input)
        except ValueError:
            print("Error: Arguments must be integers.")
