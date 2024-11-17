from pipefilter import pipe_filter, Cache, Stall
from contextlib import ExitStack


@pipe_filter
def add(a, b):
    return a + b


if __name__ == '__main__':
    with ExitStack() as ctx:
        q1 = Cache([1, 2, 3, 4, Stall(), 5, 6], ctx=ctx)
        q2 = Cache([6, Stall(), 5, 4, 3, 2, 1], ctx=ctx)
        values = add(q1, q2)
        for item in values:
            print(item)
