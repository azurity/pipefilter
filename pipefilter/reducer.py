from typing import List
from collections.abc import Iterable
from decorator import decorator
from .filter import pipe_filter
from .state import State
from .cache import Cache
from contextlib import ExitStack


@decorator
def pipe_reducer(fn, *args: List[Iterable]):
    value = None
    impl = pipe_filter(fn, cacheless=True)(*args)
    for item in impl:
        value = item
    return State(value)
