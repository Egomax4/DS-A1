from multiprocessing import Process,Pool,Manager
from mapper import map,tokenize
from reducer import reduce,d
from math import log
import sys

Mapper_count = 6
Reducer_count = 2

# f = open("testcases.txt")
def run_page_rank(input_path, output_path):
    f = open(input_path)
    Nodes = f.readlines()
    f.close()
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


    for epoch in range(10):
        with open(f"page_ranks/page_ranks_{epoch}") as f:
            prev_file_tokenized = [i.split() for i in f.readlines()] 
            prev_file = {}
            for i in prev_file_tokenized:
                prev_file[i[0]]=float(i[1])
            
        # print(prev_file)
        for i in page_ranks:
            page_ranks[i] = (1-d)/N
        Reducers = [Process(target=reduce,args=(reduce_queue,page_ranks,queue_lock,page_rank_lock)) for _ in range(Reducer_count)]
        for Reducer in Reducers:
            Reducer.start()

        Mappers = Pool(Mapper_count)
        Mappers.starmap(map,[(doc,reduce_queue,prev_file) for doc in Nodes])
        Mappers.close()
        Mappers.join()

        reduce_queue.put(None)
        for Reducer in Reducers:
            Reducer.join()
        while(not reduce_queue.empty()):
            reduce_queue.get()
        # print(page_ranks)
        with open(f"page_ranks/page_ranks_{epoch+1}","w") as f:
            for key in page_ranks:
                f.write(f"{int(key)}\t{page_ranks[key]:.6f}\n")

    with open(f"page_ranks/page_ranks_10") as f:
        with open(output_path,"w") as f1:
            # for line in f.readlines
            f1.writelines(f.readlines())

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python main.py <input_file> <output_file>")
        sys.exit(1)
    
    run_page_rank(sys.argv[1], sys.argv[2])