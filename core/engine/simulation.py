from collections.abc import MutableSequence
from typing import Self

from pydantic import ConfigDict, Field

from core.base import FrozenModel
from core.engine.queue import EventQueue
from core.events import Event
from core.market import HousingMarket
from core.registry import SignalRegistry


class SimulationEngine(FrozenModel):
    model_config = ConfigDict(frozen=False)

    market: HousingMarket
    queue: EventQueue
    registry: SignalRegistry
    now: float = 0.0
    event_log: MutableSequence[Event] = Field(default_factory=list[Event])

    def step(self) -> Self:
        event, queue = self.queue.pop()

        if not event.is_valid(self.market):
            return self.model_copy(update={"queue": queue, "now": event.time})

        market, emitted = event.apply(self.market)
        invalid = self.registry.propagate(event.invalidates())

        new_events = emitted

        for entity in market.entities:
            if entity.DEPENDS_ON & invalid:
                decisions = entity.decide(market, event.time)
                new_events.extend(decisions)

        for e in new_events:
            queue = queue.push(e)

        self.event_log.append(event)
        return self.model_copy(
            update={
                "market": market,
                "queue": queue,
                "now": event.time,
            }
        )
