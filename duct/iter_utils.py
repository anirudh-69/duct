from itertools import islice, tee

def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = list(islice(it, size))
        if not chunk:
            break
        yield chunk

def pairwise(iterable):
    a, b = tee(iterable)
    next(b, None)
    return zip(a, b)

def batch_process(iterable, batch_size, func):
    for batch in chunked(iterable, batch_size):
        yield func(batch)