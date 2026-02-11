d = 0.85

def reduce(queue,page_ranks,queue_lock,page_rank_lock):
    personal_page_ranks = {}
    while True:
        queue_lock.acquire()
        map_out = queue.get()
        queue_lock.release()
        # print(map_out)
        # print("IN:",map_out)
        if map_out==None:
            # print("I run")
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
            personal_page_ranks[neighbour] = d*val/out_degree if neighbour not in personal_page_ranks else personal_page_ranks[neighbour] + d*val/out_degree
        # for neighbour in neighbours:
        #     lock.acquire()
        #     page_ranks[neighbour] += d*val/out_degree
        #     lock.release()
        # page_ranks[map_out[0]] = (1-d)/N + d*sum([prev_file_ranks[i]/degrees[i] for i in map_out[1]])
    # print("Escape")
    page_rank_lock.acquire()
    for node in personal_page_ranks:
        page_ranks[node]+=personal_page_ranks[node]
    page_rank_lock.release()
    # print(personal_page_ranks)