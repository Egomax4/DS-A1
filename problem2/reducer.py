d = 0.85

def reduce(queue,page_ranks,lock):
    while True:
        # lock.acquire()
        map_out = queue.get()
        # lock.release()
        # print("IN:",map_out)
        if map_out==None:
            queue.put(None)
            break
        neighbours,out_degree,val = map_out
        # print(degrees)
        # print([prev_file_ranks[i]/degrees[i] for i in map_out[1]])
        # print(N)
        # print((1-d)/N)
        # print(val,neighbours,out_degree)
        # print(node,prev_file_ranks)
        for neighbour in neighbours:
            lock.acquire()
            page_ranks[neighbour] += d*val/out_degree
            lock.release()
        # page_ranks[map_out[0]] = (1-d)/N + d*sum([prev_file_ranks[i]/degrees[i] for i in map_out[1]])
