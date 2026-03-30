from collections.abc import MutableSequence
from typing import Self

from pydantic import ConfigDict, Field

from core.base import FrozenModel
from core.context import SimulationContext
from core.engine.queue import EventQueue
from core.events import EventType
from core.market import HousingMarket
from core.registry import SignalRegistry


class SimulationEngine(FrozenModel):
    model_config = ConfigDict(frozen=False)

    market: HousingMarket
    queue: EventQueue
    registry: SignalRegistry
    context: SimulationContext = Field(default_factory=SimulationContext)
    now: float = 0.0
    event_log: MutableSequence[EventType] = Field(default_factory=list[EventType])

    def step(self) -> Self:
        event, queue = self.queue.pop()

        if not event.is_valid(self.market):
            return self.model_copy(update={"queue": queue, "now": event.time})

        market, context, emitted = event.apply(self.market, self.context)
        invalid = self.registry.propagate(event.invalidates())

        new_events = list[EventType](emitted)

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
                "context": context,
                "now": event.time,
            }
        )
