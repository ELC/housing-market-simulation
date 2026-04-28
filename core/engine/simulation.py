from collections.abc import MutableSequence
from typing import Self

from pydantic import ConfigDict, Field

from core.base import FrozenModel
from core.context import SimulationContext
from core.engine.queue import EventQueue
from .result import RunResult
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

    def run(self, *, n_steps: int, max_t: float) -> RunResult:
        sim = self
        for _ in range(n_steps):
            if not sim.queue.events:
                break
            sim = sim.step()
            if sim.now >= max_t:
                break
        return RunResult(
            event_log=list(sim.event_log),
            settings=sim.market.settings,
        )

    def step(self) -> Self:
        event, queue = self.queue.pop()
        self.queue = queue
        self.now = event.time

        if not event.is_valid(self.market):
            return self

        market, context, emitted = event.apply(self.market, self.context)
        self.market = market
        self.context = context

        for e in emitted:
            queue.push(e)

        # Most events have ``invalidates=frozenset()`` (the base default), so
        # the propagation closure is empty and no entity can possibly react.
        # Short-circuiting here avoids touching every entity's ``depends_on``
        # on the hot path.
        if not event.invalidates:
            self.event_log.append(event)
            return self

        invalid = self.registry.propagate(event.invalidates)
        if not invalid:
            self.event_log.append(event)
            return self

        for entity in market.all_entities():
            if not entity.depends_on & invalid:
                continue
            for decision in entity.decide(market, event.time):
                queue.push(decision)

        self.event_log.append(event)
        return self
