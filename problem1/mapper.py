import re

def tokenize(document):
    return re.sub("[.,\":;?!()\t\n]"," ",document).lower().split()

# def map(document,combine_queue):
#     tokenized_doc = tokenize(document)[1:]
#     # print(tokenized_doc)
#     combine_queue.put([(i,1) for i in tokenized_doc])

def map(file,combine_queue,file_lock):
    while True:
        file_lock.acquire()
        doc = file.readline()
        file_lock.release()

        if not doc:
            break
        
        doc = doc.decode('utf-8')
        # print(doc)
        tokenized_doc = tokenize(doc)[1:]
        # print(tokenized_doc)
        # exit()
        combine_queue.put([(i,1) for i in tokenized_doc])