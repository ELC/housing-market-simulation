from typing import TYPE_CHECKING, ClassVar

from core.events.base import Event
from core.policies.protocol import AgentPolicy
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


class IncomePolicy(AgentPolicy):
    DEPENDS_ON: ClassVar[frozenset[Signal]] = frozenset()

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list[Event]:
        return []
