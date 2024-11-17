from dataclasses import dataclass
from typing import List, Generator, Deque
import collections
from contextlib import ExitStack
import numpy as np
import rosbag
from message_filters import ApproximateTimeSynchronizer, Subscriber
from .cache import Cache


_BagMessage = collections.namedtuple('BagMessage', 'topic message timestamp')


@dataclass
class BagInfo:
    filename: str
    topic: str
    data_class: type


class _Source:
    def __init__(self, infos: List[BagInfo], queue_size: int = 100, slop: float = 0.1, ctx: ExitStack = None):
        self.data = None
        self.bags: List[rosbag.Bag] = [
            rosbag.Bag(info.filename) for info in infos]
        self.iters: List[Generator[_BagMessage, None, None]] = [
            bag.read_messages(info.topic) for bag, info in zip(self.bags, infos)]
        self.queues: List[Deque[_BagMessage]] = [
            collections.deque() for _ in infos]
        self.subscribers: List[Subscriber] = [Subscriber(
            info.topic, info.data_class) for info in infos]
        self.syncer = ApproximateTimeSynchronizer(
            self.subscribers, queue_size, slop)
        self.syncer.registerCallback(self._callback)
        self.topics = [info.topic for info in infos]
        if ctx is not None:
            ctx.enter_context(self)
        self.contexted = ctx is not None

    def _callback(self, *args):
        self.data = dict(zip(self.topics, args))

    def next(self):
        while True:
            # try return
            if self.data is not None:
                ret, self.data = self.data, None
                return ret
            # fill all seq
            for i in range(len(self.queues)):
                if len(self.queues[i]) == 0:
                    self.queues[i].append(next(self.iters[i]))
            # signal oldest
            i: int = np.argmin([seq[0].timestamp.to_sec()
                               for seq in self.queues])
            msg = self.queues[i].popleft().message
            self.subscribers[i].signalMessage(msg)

    def __iter__(self):
        return self

    def __next__(self):
        return self.next()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for bag in self.bags:
            bag.close()

    def __call__(self, ctx: ExitStack):
        if not self.contexted and ctx is not None:
            ctx.enter_context(self)
            self.contexted = True
        return self


def bag_pipe(infos: List[BagInfo], queue_size: int = 100, slop: float = 0.1, ctx: ExitStack = None):
    return Cache(_Source(infos, queue_size, slop), ctx, True)
