import sys
from functools import reduce
from itertools import islice
from math import ceil
from pathlib import Path

import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
RANK = comm.Get_rank()
NUM_PROCS = comm.Get_size()

TAG_SCAN = 100
TAG_LABEL = 200


def scan_data(path: Path):
    edges_to_send = [[] for _ in range(NUM_PROCS)]
    labels_to_send = [set() for _ in range(NUM_PROCS)]
    with path.open() as f:
        header = f.readline().strip().split()
        num_nodes = int(header[0])
        num_edges = int(header[1])

        lines_per_proc = ceil(num_edges / NUM_PROCS)
        start_line = lines_per_proc * RANK
        if RANK == NUM_PROCS - 1:
            end_line = num_edges - 1  # inclusive
        else:
            end_line = start_line + lines_per_proc - 1  # inclusive

        nodes_per_proc = ceil(num_nodes / NUM_PROCS)
        start_node = nodes_per_proc * RANK
        if start_node >= num_nodes:
            start_node = num_nodes
            end_node = num_nodes - 1
        else:
            end_node = min(start_node + nodes_per_proc - 1, num_nodes - 1)
        if RANK == NUM_PROCS - 1:
            end_node = num_nodes - 1  # inclusive

        # print(f"Process {RANK}'s nodes: {start_node} to {end_node}")
        # print(f"Process {RANK}'s lines: {start_line} to {end_line}")

        local_edges = []

        # https://stackoverflow.com/a/36854340
        for line in islice(f, start_line, end_line + 1):
            items = line.strip().split()
            if not items:
                continue
            src = int(items[0])
            dst = int(items[1])
            # print(f"Process {RANK} got edge {src}-{dst}")

            if src < start_node or src > end_node:
                edges_to_send[src // nodes_per_proc].extend([src, dst])
                if (
                    not (dst < start_node or dst > end_node)
                    and dst not in labels_to_send[src // nodes_per_proc]
                ):
                    labels_to_send[src // nodes_per_proc].add(dst)
            else:
                local_edges.extend([src, dst])
            if dst < start_node or dst > end_node:
                edges_to_send[dst // nodes_per_proc].extend([dst, src])
                if (
                    not (src < start_node or src > end_node)
                    and src not in labels_to_send[dst // nodes_per_proc]
                ):
                    labels_to_send[dst // nodes_per_proc].add(src)
            else:
                local_edges.extend([dst, src])

    send_request_lengths = np.array(
        [len(edges) for edges in edges_to_send], dtype=np.int32
    )
    recv_request_lengths = np.empty_like(send_request_lengths, dtype=np.int32)
    comm.Alltoall(send_request_lengths, recv_request_lengths)

    # print(send_request_lengths, flush=True)
    # print(recv_request_lengths, flush=True)

    requests = []
    recv_buffers = [
        np.empty(recv_request_lengths[i], dtype=np.int32)
        for i in range(NUM_PROCS)
    ]
    for i in range(NUM_PROCS):
        if i == RANK:
            continue
        requests.append(
            comm.Irecv((recv_buffers[i], MPI.INT32_T), source=i, tag=TAG_SCAN)
        )
    for i in range(NUM_PROCS):
        if i == RANK:
            continue
        requests.append(
            comm.Isend(
                (np.array(edges_to_send[i], dtype=np.int32), MPI.INT32_T),
                dest=i,
                tag=TAG_SCAN,
            )
        )

    local_edges = np.array(local_edges, dtype=np.int32)

    MPI.Request.Waitall(requests)
    for result in recv_buffers:
        if result.shape[0] == 0:
            continue
        local_edges = np.concat((local_edges, result))
        masked_edges = (result[1::2] < start_node) | (result[1::2] > end_node)
        for num, dst_proc in zip(
            result[::2][masked_edges],
            result[1::2][masked_edges] // nodes_per_proc,
        ):
            labels_to_send[dst_proc].add(num)

    labels_to_send = [
        np.array(list(labels), dtype=np.int32) for labels in labels_to_send
    ]

    return (
        local_edges,
        labels_to_send,
        start_node,
        end_node,
        nodes_per_proc,
        num_nodes,
    )


def label_propagate(
    labels: np.ndarray,
    local_edges: np.ndarray,
    labels_to_send: list[np.ndarray],
    start_node: int,
    end_node: int,
    nodes_per_proc: int,
):
    # print(f"Process {RANK} {labels=}")
    # print([labels.shape for labels in labels_to_send], flush=True)
    send_labels_lengths = np.array(
        [labels.shape[0] * 2 for labels in labels_to_send], dtype=np.int32
    )
    recv_labels_lengths = np.empty_like(send_labels_lengths, dtype=np.int32)
    comm.Alltoall(send_labels_lengths, recv_labels_lengths)

    requests = []
    recv_buffers = [
        np.empty(recv_labels_lengths[i], dtype=np.int32)
        for i in range(NUM_PROCS)
    ]
    for i in range(NUM_PROCS):
        if i == RANK:
            continue
        requests.append(
            comm.Irecv((recv_buffers[i], MPI.INT32_T), source=i, tag=TAG_LABEL)
        )
    for i, label_idxs in enumerate(labels_to_send):
        if i == RANK:
            continue
        data = np.empty(2 * label_idxs.shape[0], dtype=np.int32)
        data[::2] = label_idxs
        data[1::2] = labels[label_idxs - start_node]
        requests.append(comm.Isend((data, MPI.INT32_T), dest=i, tag=TAG_LABEL))
    received_labels = {}
    MPI.Request.Waitall(requests)
    for result in recv_buffers:
        if result.shape[0] == 0:
            continue
        for i in range(0, len(result), 2):
            received_labels[result[i]] = result[i + 1]

    # print(received_labels)

    changed = False
    for i in range(0, len(local_edges), 2):
        if (
            local_edges[i + 1] >= start_node
            and local_edges[i + 1] <= end_node
            and labels[local_edges[i + 1] - start_node]
            < labels[local_edges[i] - start_node]
        ):
            changed = True
            labels[local_edges[i] - start_node] = labels[
                local_edges[i + 1] - start_node
            ]
        elif (
            (local_edges[i + 1] < start_node or local_edges[i + 1] > end_node)
            and local_edges[i + 1] in received_labels
            and received_labels[local_edges[i + 1]]
            < labels[local_edges[i] - start_node]
        ):
            changed = True
            labels[local_edges[i] - start_node] = received_labels[
                local_edges[i + 1]
            ]

    return comm.allreduce(changed, op=MPI.LOR)


def run_connected_components(path: Path):
    # dataPath = Path("./data.txt")
    (
        local_edges,
        labels_to_send,
        start_node,
        end_node,
        nodes_per_proc,
        num_nodes,
    ) = scan_data(path)
    # print(f"Process {RANK} {local_edges=}")
    # print(f"Process {RANK} {labels_to_send=}")
    # print(f"Process {RANK} {start_node=}")
    # print(f"Process {RANK} {end_node=}")
    # print(f"Process {RANK} {nodes_per_proc=}")

    labels = np.arange(start_node, end_node + 1, dtype=np.int32)

    count = 0
    while label_propagate(
        labels,
        local_edges,
        labels_to_send,
        start_node,
        end_node,
        nodes_per_proc,
    ):
        count += 1

    if (all_labels := comm.gather(set[int](labels))) is not None:
        print(len(reduce(lambda x, y: x.union(y), all_labels)))


run_connected_components(Path(sys.argv[1]))
