def reduce(queue,cur_dict,page_count,dict_lock,page_count_lock,queue_lock):
    personal_dict = {}
    personal_page_count = 0
    while True:
        queue_lock.acquire()
        map_out = queue.get()
        queue_lock.release()
        # print("IN:",map_out)
        if map_out==None:
            queue.put(None)
            break

        for key in map_out:
            personal_dict[key] = personal_dict[key]+map_out[key] if key in personal_dict else map_out[key]
            
        personal_page_count+=1
        # print("DICT",cur_dict)
    
    dict_lock.acquire()
    for key in personal_dict:
        cur_dict[key] = cur_dict[key]+personal_dict[key] if key in cur_dict else personal_dict[key]
    dict_lock.release()

    page_count_lock.acquire()
    page_count.value += personal_page_count
    page_count_lock.release()