import heapq
from typing import Self, Sequence

from core.base import FrozenModel
from core.events import EventType
from core.market import HousingMarket
from core.registry import SignalRegistry

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


class SimulationEngine(FrozenModel):
    market: HousingMarket
    queue: EventQueue
    registry: SignalRegistry
    now: float = 0.0
    event_log: Sequence[EventType] = ()

    def step(self) -> Self:
        event, queue = self.queue.pop()

        if not event.validate(self.market):
            return self.model_copy(update={"queue": queue, "now": event.time})

        market, emitted = event.apply(self.market)
        invalid = self.registry.propagate(event.invalidates())

        new_events: list[EventType] = list(emitted)

        for entity in market.entities:
            if entity.DEPENDS_ON & invalid:
                decisions = entity.decide(market, event.time)
                new_events.extend(decisions)

        for e in new_events:
            queue = queue.push(e)

        return self.model_copy(
            update={
                "market": market,
                "queue": queue,
                "now": event.time,
                "event_log": (*self.event_log, event),
            }
        )
