# from multiprocessing import Process,Pool,Manager
# from mapper import map
# from reducer import reduce
# from combiner import combine
# from math import log

Mapper_count = 3
Combiner_count = 3
Reducer_count = 2

# f = open("testcases.txt")
# # docs = f.readlines()

# manager = Manager()
# combine_queue = manager.Queue()
# reduce_queue = manager.Queue()
# doc_freq = manager.dict()
# num_docs = manager.Value("N",0)
# dict_lock = manager.Lock()
# file_lock = manager.Lock()

# Reducers = [Process(target=reduce,args=(reduce_queue,doc_freq,num_docs,dict_lock)) for _ in range(Reducer_count)]
# for Reducer in Reducers:
#     Reducer.start()

# Combiners = [Process(target=combine,args=(combine_queue,reduce_queue)) for _ in range(Combiner_count)]
# for Combiner in Combiners:
#     Combiner.start()

# # Mappers = Pool(Mapper_count)
# # Mappers.starmap(map,[(doc,combine_queue) for doc in docs])
# # Mappers.close()
# # Mappers.join()

# Mappers = [Process(target=map,args=(f,combine_queue,file_lock))]
# for Mapper in Mappers:
#     Mapper.start()

# for Mapper in Mappers:
#     Mapper.join()

# combine_queue.put(None)
# for Combiner in Combiners:
#     Combiner.join()

# reduce_queue.put(None)
# for Reducer in Reducers:
#     Reducer.join()

# # print(num_docs)
# # print(doc_freq)
# for key in sorted(doc_freq.keys()):
#     print(f"{key}\t{log(num_docs.value/doc_freq[key])}")

import sys
from multiprocessing import Process, Manager
from mapper import map
from reducer import reduce
from combiner import combine
from math import log

# ... (Keep your counts and imports) ...

def run_map_reduce(input_path, output_path):
    manager = Manager()
    combine_queue = manager.Queue()
    reduce_queue = manager.Queue()
    doc_freq = manager.dict()
    num_docs = manager.Value("i", 0) # Fixed typecode to 'i' for integer
    dict_lock = manager.Lock()
    file_lock = manager.Lock()
    page_count_lock = manager.Lock()
    queue_lock = manager.Lock()

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
        for m in Mappers: m.start()

        # Wait for Mappers to finish
        for m in Mappers: m.join()

        # Shutdown sequence
        for _ in range(Combiner_count): combine_queue.put(None)
        for c in Combiners: c.join()

        for _ in range(Reducer_count): reduce_queue.put(None)
        for r in Reducers: r.join()

    # Write results to the output file
    with open(output_path, "w") as out_f:
        for key in sorted(doc_freq.keys()):
            idf = log(num_docs.value / doc_freq[key])
            out_f.write(f"{key}\t{idf}\n")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_file> <output_file>")
        sys.exit(1)
    
    run_map_reduce(sys.argv[1], sys.argv[2])