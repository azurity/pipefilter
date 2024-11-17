from typing import Callable, List, Any
from collections.abc import Iterable
from decorator import decorator
from .state import State
from .cache import Cache


class Stall:
    pass


class _Filter:
    def __init__(self, fn: Callable[..., Any], *args: List[Iterable]):
        self.fn = fn
        self.seqs = [(iter(item) if isinstance(item, Cache) or isinstance(item, State)
                      else iter(Cache(item))) for item in args]
        self.cache = [Stall() for _ in args]

    def __iter__(self):
        return self

    def __next__(self):
        for i, seq in enumerate(self.seqs):
            while isinstance(self.cache[i], Stall):
                self.cache[i] = next(seq)
        data, self.cache = self.cache, [Stall() for _ in self.seqs]
        return self.fn(*data)


@decorator
def pipe_filter(fn, cacheless=False, *args: List[Iterable]):
    if cacheless:
        return _Filter(fn, *args)
    else:
        return Cache(_Filter(fn, *args))
