from abc import abstractmethod
from typing import TYPE_CHECKING

from core.base import FrozenModel
from core.events.base import Event
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


class AgentPolicy(FrozenModel):
    @property
    @abstractmethod
    def DEPENDS_ON(self) -> frozenset[Signal]: ...

    @abstractmethod
    def decide(
        self,
        agent: "Agent",
        market: "HousingMarket",
        now: float,
    ) -> list[Event]: ...


class CompositeAgentPolicy(AgentPolicy):
    policies: tuple[AgentPolicy, ...]

    @property
    def DEPENDS_ON(self) -> frozenset[Signal]:
        deps: set[Signal] = set()
        for p in self.policies:
            deps |= p.DEPENDS_ON
        return frozenset(deps)

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list[Event]:
        events: list[Event] = []
        for p in self.policies:
            events.extend(p.decide(agent, market, now))
        return events
