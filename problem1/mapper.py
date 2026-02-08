import re

def tokenize(document):
    return re.sub("[.,\":;?!()\t\n]"," ",document).lower().split()

def map(document,combine_queue):
    tokenized_doc = tokenize(document)[1:]
    # print(tokenized_doc)
    combine_queue.put([(i,1) for i in tokenized_doc])