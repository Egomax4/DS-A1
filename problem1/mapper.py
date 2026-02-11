import re

def tokenize(document):
    return re.sub("[.,\":;?!()\t\n]"," ",document).lower().split()

def map(file,combine_queue,file_lock):
    while True:
        file_lock.acquire()
        doc = file.readline()
        file_lock.release()

        if not doc:
            break
        
        doc = doc.decode('utf-8')
        tokenized_doc = tokenize(doc)[1:]
        combine_queue.put([(i,1) for i in tokenized_doc])