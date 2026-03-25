from typing import TYPE_CHECKING, FrozenSet, Protocol, runtime_checkable

from core.base import FrozenModel
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


@runtime_checkable
class AgentPolicy(Protocol):
    DEPENDS_ON: FrozenSet[Signal]

    def decide(
        self,
        agent: "Agent",
        market: "HousingMarket",
        now: float,
    ) -> list: ...


class CompositeAgentPolicy(FrozenModel):
    policies: tuple[AgentPolicy, ...]

    @property
    def DEPENDS_ON(self) -> FrozenSet[Signal]:
        deps: set[Signal] = set()
        for p in self.policies:
            deps |= p.DEPENDS_ON
        return frozenset(deps)

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list:
        events: list = []
        for p in self.policies:
            result = p.decide(agent, market, now)
            events.extend(result)
        return events
