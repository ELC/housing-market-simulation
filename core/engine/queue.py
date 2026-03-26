import heapq
from typing import Self, Sequence

from core.base import FrozenModel
from core.events import EventType

QueueItem = tuple[float, int, EventType]


class EventQueue(FrozenModel):
    events: Sequence[QueueItem] = ()
    counter: int = 0

    def push(self, e: EventType) -> Self:
        items = list(self.events)
        heapq.heappush(items, (e.time, self.counter, e))
        return self.model_copy(
            update={"events": tuple(items), "counter": self.counter + 1}
        )

    def pop(self) -> tuple[EventType, Self]:
        items = list(self.events)
        _, _, event = heapq.heappop(items)
        return event, self.model_copy(update={"events": tuple(items)})
