def combine(combine_queue,reduce_queue):
    while True:
        map_out = combine_queue.get()
        if map_out == None:
            combine_queue.put(None)
            break    
        combined_dict = {}
        for key,value in map_out:
            if key in combined_dict:
                # combined_dict[key]+=1
                continue
            else:
                combined_dict[key]=1
        reduce_queue.put(combined_dict)