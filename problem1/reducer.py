def reduce(queue,cur_dict,page_count,lock):
    while True:
        lock.acquire()
        map_out = queue.get()
        lock.release()
        # print("IN:",map_out)
        if map_out==None:
            queue.put(None)
            break

        for key in map_out:
            lock.acquire()
            cur_dict[key] = cur_dict[key]+map_out[key] if key in cur_dict else map_out[key]
            lock.release()

        # print("DICT",cur_dict)
        lock.acquire()
        page_count.value += 1
        lock.release()