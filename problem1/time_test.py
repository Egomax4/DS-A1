import sys
from multiprocessing import Process, Manager
from mapper import map
from reducer import reduce
from combiner import combine
from math import log
import time

Mapper_count = 2
Combiner_count = 4
Reducer_count = 2
# ... (Keep your counts and imports) ...

def run_map_reduce(input_path, output_path):
    start_job = time.time()

    init_start = time.time()
    manager = Manager()
    combine_queue = manager.Queue()
    reduce_queue = manager.Queue()
    doc_freq = manager.dict()
    num_docs = manager.Value("i", 0) # Fixed typecode to 'i' for integer
    dict_lock = manager.Lock()
    file_lock = manager.Lock()
    page_count_lock = manager.Lock()
    queue_lock = manager.Lock()
    init_end = time.time()

    # Open the input file
    with open(input_path, "rb", buffering=0) as f:
        # Start Reducers
        Reducers = [Process(target=reduce, args=(reduce_queue, doc_freq, num_docs, dict_lock,page_count_lock,queue_lock)) 
                    for _ in range(Reducer_count)]
        for r in Reducers: r.start()

        # Start Combiners
        Combiners = [Process(target=combine, args=(combine_queue, reduce_queue)) 
                     for _ in range(Combiner_count)]
        for c in Combiners: c.start()

        # Start Mappers (passing the file object f)
        Mappers = [Process(target=map, args=(f, combine_queue, file_lock)) 
                   for _ in range(Mapper_count)]
        
        map_start = time.time()
        for m in Mappers: m.start()
        reduce_start = time.time()

        # Wait for Mappers to finish
        for m in Mappers: m.join()
        map_end = time.time()

        # Shutdown sequence

        for _ in range(Combiner_count): combine_queue.put(None)
        for c in Combiners: c.join()

        for _ in range(Reducer_count): reduce_queue.put(None)
        for r in Reducers: r.join()
        reduce_end = time.time()

        end_job = time.time()
    # Write results to the output file
    with open(output_path, "w") as out_f:
        for key in sorted(doc_freq.keys()):
            idf = log(num_docs.value / doc_freq[key])
            out_f.write(f"{key}\t{idf}\n")

    with open("time_calcs.txt","w") as f:
        f.write(f"Initialization: {init_end - init_start:.4f}s\n")
        f.write(f"Mapper Time: {map_end - map_start:.4f}s\n")
        f.write(f"Reducer/Shuffle Time: {reduce_end - reduce_start:.4f}s\n")
        f.write(f"Total Time: {end_job - start_job:.4f}s\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_file> <output_file>")
        sys.exit(1)
    
    run_map_reduce(sys.argv[1], sys.argv[2])