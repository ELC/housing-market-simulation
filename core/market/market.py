from typing import Self, Sequence

from core.agent import Agent
from core.base import FrozenModel
from core.entity import Entity
from core.events import Bid
from core.house import House
from core.settings import SimulationSettings


class HousingMarket(FrozenModel):
    entities: Sequence[Entity]
    settings: SimulationSettings
    pending_bids: Sequence[Bid] = ()

    @property
    def agents(self) -> tuple[Agent, ...]:
        return tuple(e for e in self.entities if isinstance(e, Agent))

    @property
    def houses(self) -> tuple[House, ...]:
        return tuple(e for e in self.entities if isinstance(e, House))

    @property
    def agent_map(self) -> dict[str, Agent]:
        return {a.id: a for a in self.agents}

    @property
    def house_map(self) -> dict[str, House]:
        return {h.id: h for h in self.houses}

    def update_entities(self, updates: dict[str, Entity]) -> Self:
        updated = tuple(updates.get(e.id, e) for e in self.entities)
        return self.model_copy(update={"entities": updated})
