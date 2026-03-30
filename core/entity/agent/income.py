from typing import TYPE_CHECKING

from core.entity.agent.protocol import AgentPolicy
from core.events import EventType
from core.signals import Signal

if TYPE_CHECKING:
    from core.entity.agent.agent import Agent
    from core.market import HousingMarket


class IncomePolicy(AgentPolicy):
    @property
    def DEPENDS_ON(self) -> frozenset[Signal]:
        return frozenset()

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list[EventType]:
        return []
