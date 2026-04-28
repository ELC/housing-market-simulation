import heapq
from collections.abc import Iterator
from itertools import count
from typing import Self

from pydantic import BaseModel, ConfigDict, Field

from core.events import EventType

QueueItem = tuple[float, int, EventType]


class EventQueue(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    events: list[QueueItem] = Field(default_factory=list[QueueItem])
    counter: Iterator[int] = Field(default_factory=count)

    def push(self, e: EventType) -> Self:
        heapq.heappush(self.events, (e.time, next(self.counter), e))
        return self

    def pop(self) -> tuple[EventType, Self]:
        _, _, event = heapq.heappop(self.events)
        return event, self
