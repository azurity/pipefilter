from typing import List, Callable, Any
import collections
from collections.abc import Iterable
from contextlib import ExitStack


class Cache:
    def __init__(self, impl: Iterable, ctx: ExitStack = None, sub_context=False):
        self.impl = iter(impl)
        self.cache = collections.deque()
        self.begin = 0
        self.end = 0
        self.finish = False
        self.iters: List[_CacheIterator] = []
        self.contexted = False
        self.sub_context = sub_context
        self.__call__(ctx)

    def __iter__(self):
        ret = _CacheIterator(self)
        self.iters.append(ret)
        return ret

    def get(self, i: int):
        if i < self.begin:
            raise StopIteration()
        if self.finish and i >= self.end:
            raise StopIteration()
        while i >= self.end:
            try:
                self.cache.append(next(self.impl))
                self.end += 1
            except StopIteration:
                self.finish = True
                raise StopIteration()
        ret = self.cache[i - self.begin]
        new_begin = min([item.index for item in self.iters])
        while new_begin > self.begin:
            self.cache.popleft()
            self.begin += 1
        return ret

    def release(self, item):
        self.iters.remove(item)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.finish = True
        self.cache.clear()
        self.begin = self.end

    def __call__(self, ctx: ExitStack):
        if not self.contexted and ctx is not None:
            if self.sub_context:
                self.impl(ctx)
            ctx.enter_context(self)
            self.contexted = True
        return self


class _CacheIterator:
    def __init__(self, impl: Cache):
        self.impl = impl
        self.index = 0

    def __iter__(self):
        return self

    def clone(self):
        if self.impl is None:
            return _CacheIterator(None)
        return self.impl.__iter__()

    def release(self):
        if self.impl is None:
            return
        self.impl.release(self)
        self.impl = None

    def __next__(self):
        if self.impl is None:
            raise StopIteration()
        index = self.index
        self.index += 1
        ret = self.impl.get(index)
        return ret
