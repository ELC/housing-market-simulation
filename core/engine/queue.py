import heapq
from collections.abc import Sequence
from typing import Self

from core.base import FrozenModel
from core.events import Event

QueueItem = tuple[float, int, Event]


class EventQueue(FrozenModel):
    events: Sequence[QueueItem] = ()
    counter: int = 0

    def push(self, e: Event) -> Self:
        items = list(self.events)
        heapq.heappush(items, (e.time, self.counter, e))
        return self.model_copy(update={"events": tuple(items), "counter": self.counter + 1})

    def pop(self) -> tuple[Event, Self]:
        items = list(self.events)
        _, _, event = heapq.heappop(items)
        return event, self.model_copy(update={"events": tuple(items)})
