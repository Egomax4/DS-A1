import re

def tokenize(document):
    return re.sub("[.,\":;?!()\t\n]"," ",document).lower().split()

def map(document,reduce_queue,prev_file):
    tokenized_doc = tokenize(document)
    # print(tokenized_doc)
    # print(prev_file)
    reduce_queue.put((tokenized_doc[1:],len(tokenized_doc)-1,prev_file[tokenized_doc[0]]))