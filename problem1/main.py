from multiprocessing import Process,Pool,Manager
from mapper import map
from reducer import reduce
from combiner import combine
from math import log

Mapper_count = 5
Combiner_count = 3
Reducer_count = 2

f = open("testcases.txt")
docs = f.readlines()

manager = Manager()
combine_queue = manager.Queue()
reduce_queue = manager.Queue()
doc_freq = manager.dict()
num_docs = manager.Value("N",0)
dict_lock = manager.Lock()

Reducers = [Process(target=reduce,args=(reduce_queue,doc_freq,num_docs,dict_lock)) for _ in range(Reducer_count)]
for Reducer in Reducers:
    Reducer.start()

Combiners = [Process(target=combine,args=(combine_queue,reduce_queue)) for _ in range(Combiner_count)]
for Combiner in Combiners:
    Combiner.start()

Mappers = Pool(Mapper_count)
Mappers.starmap(map,[(doc,combine_queue) for doc in docs])
Mappers.close()
Mappers.join()

combine_queue.put(None)
for Combiner in Combiners:
    Combiner.join()

reduce_queue.put(None)
for Reducer in Reducers:
    Reducer.join()

# print(num_docs)
# print(doc_freq)
for key in sorted(doc_freq.keys()):
    print(f"{key}\t{log(num_docs.value/doc_freq[key])}")