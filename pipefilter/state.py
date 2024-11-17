from contextlib import ExitStack


class State:
    def __init__(self, value, ctx: ExitStack = None):
        self.value = value
        self.finish = False
        if ctx is not None:
            ctx.enter_context(self)
        self.contexted = ctx is not None

    def __iter__(self):
        return self

    def __next__(self):
        if self.finish:
            raise StopIteration()
        return self.value

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.finish = True

    def __call__(self, ctx: ExitStack):
        if not self.contexted and ctx is not None:
            ctx.enter_context(self)
            self.contexted = True
        return self
