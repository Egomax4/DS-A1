from multiprocessing import Process,Pool,Manager
from mapper import map
from reducer import reduce,d
import sys
import time

Mapper_count = 6
Reducer_count = 2

# f = open("testcases.txt")
def run_page_rank(input_path, output_path):
    f = open(input_path)
    Nodes = f.readlines()
    f.close()

    init_start = time.time()
    N = len(Nodes)
    with open(f"page_ranks/page_ranks_0","w") as f:
        for i in range(N):
            f.write(f"{i}\t{1/N}\n")

    manager = Manager()
    reduce_queue = manager.Queue()
    page_ranks = manager.dict()
    for i in range(N):
        page_ranks[f"{i}"]=0
    queue_lock = manager.Lock()
    page_rank_lock = manager.Lock()
    init_end = time.time()

    with open("time_calcs.txt","a") as f:
        f.write(f"Global Initialization: {init_end-init_start:.4}s\n")

    times = []

    for epoch in range(10):
        epoch_start_time = time.time()
        read_start = time.time()
        with open(f"page_ranks/page_ranks_{epoch}") as f:
            prev_file_tokenized = [i.split() for i in f.readlines()] 
            prev_file = {}
            for i in prev_file_tokenized:
                prev_file[i[0]]=float(i[1])
        read_end = time.time()
        # print(prev_file)

        epoch_init_start = time.time()
        for i in page_ranks:
            page_ranks[i] = (1-d)/N
        Reducers = [Process(target=reduce,args=(reduce_queue,page_ranks,queue_lock,page_rank_lock)) for _ in range(Reducer_count)]
        for Reducer in Reducers:
            Reducer.start()

        Mappers = Pool(Mapper_count)
        epoch_init_end = time.time()

        Mappers.starmap(map,[(doc,reduce_queue,prev_file) for doc in Nodes])
        Mappers.close()
        Mappers.join()

        reduce_queue.put(None)
        for Reducer in Reducers:
            Reducer.join()
        while(not reduce_queue.empty()):
            reduce_queue.get()
        # print(page_ranks)
        write_start = time.time()
        with open(f"page_ranks/page_ranks_{epoch+1}","w") as f:
            for key in page_ranks:
                f.write(f"{int(key)}\t{page_ranks[key]:.6f}\n")
        write_end = time.time()
        epoch_end_time = time.time()

        times.append(epoch_end_time-epoch_start_time)
        with open("time_calcs.txt","a") as f:
            f.write(f"Epoch: {epoch+1}\n")
            f.write(f"Read time: {read_end-read_start:.4}s\n")
            f.write(f"Write time: {write_end-write_start:.4}s\n")
            f.write(f"Init time: {init_end-init_start:.4}s\n")
            f.write(f"Total time: {epoch_end_time-epoch_start_time:.4}s\n\n")

    with open(f"page_ranks/page_ranks_10") as f:
        with open(output_path,"w") as f1:
            # for line in f.readlines
            f1.writelines(f.readlines())

    with open("time_calcs.txt","a") as f:
        f.write(f"Total time: {sum(times)}\nAverage Time: {sum(times)/len(times)}\n")
        
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_file> <output_file>")
        sys.exit(1)
    
    run_page_rank(sys.argv[1], sys.argv[2])