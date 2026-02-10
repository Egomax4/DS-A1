import random

def generate_pagerank_data(filename, n_nodes, edge_probability=0.05):
    """
    Generates a graph for PageRank testing.
    Format: node_id target1,target2,target3
    """
    with open(filename, "w") as f:
        for node in range(n_nodes):
            targets = []
            for potential_target in range(n_nodes):
                # Avoid self-loops for basic tests, though PageRank handles them
                if node == potential_target:
                    continue
                
                if random.random() < edge_probability:
                    targets.append(str(potential_target))
            
            # Format: node_id<space>target1,target2...
            # Even if targets is empty, we list the node to ensure it exists in the graph
            line = f"{node} {','.join(targets)}\n"
            f.write(line)

if __name__ == "__main__":
    N_NODES = 100
    PROBABILITY = 0.1  # 10% chance of an edge between any two nodes
    OUTPUT_FILE = "graph.txt"
    
    generate_pagerank_data(OUTPUT_FILE, N_NODES, PROBABILITY)
    print(f"Generated graph with {N_NODES} nodes in {OUTPUT_FILE}")