from collections.abc import Mapping, Sequence
from functools import cached_property
from typing import Self

from core.agent import Agent
from core.base import FrozenModel
from core.events import Bid
from core.house import House
from core.settings import SimulationSettings


class HousingMarket(FrozenModel):
    entities: Sequence[House | Agent]
    settings: SimulationSettings
    pending_bids: Sequence[Bid] = ()

    @cached_property
    def agents(self) -> tuple[Agent, ...]:
        return tuple(e for e in self.entities if isinstance(e, Agent))

    @cached_property
    def houses(self) -> tuple[House, ...]:
        return tuple(e for e in self.entities if isinstance(e, House))

    @cached_property
    def agent_map(self) -> dict[str, Agent]:
        return {a.id: a for a in self.agents}

    @cached_property
    def house_map(self) -> dict[str, House]:
        return {h.id: h for h in self.houses}

    def update_entities(self, updates: Mapping[str, House | Agent]) -> Self:
        updated = tuple(updates.get(e.id, e) for e in self.entities)
        return self.model_copy(update={"entities": updated})
