from __future__ import annotations

import random
from typing import TYPE_CHECKING, ClassVar, Never

from core.entity.agent import Agent
from core.entity.agent.protocol import AgentPolicy
from core.events.base import ApplyResult, Event
from core.events.income import AgentIncomeReceived
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class AgentEntered(Event):
    agent_id: str
    agent_name: str
    policy: AgentPolicy

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[AgentIncomeReceived | AgentLeft | AgentEntered]:
        agent = Agent(id=self.agent_id, name=self.agent_name, policy=self.policy)
        new_market = market.update_entities({agent.id: agent})

        income = AgentIncomeReceived(
            time=self.time,
            agent_id=agent.id,
            amount=AgentIncomeReceived.compute_salary(agent),
        )
        left = AgentLeft(
            time=self.time + agent.max_homeless_periods,
            agent_id=agent.id,
        )
        next_interval = random.expovariate(1 / market.settings.migration_interval)  # noqa: S311
        next_id, next_name = next(Agent.identity)
        next_entry = AgentEntered(
            time=self.time + next_interval,
            agent_id=next_id,
            agent_name=next_name,
            policy=self.policy,
        )

        return new_market, context, [income, left, next_entry]


class AgentLeft(Event):
    agent_id: str

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def is_valid(self, market: HousingMarket) -> bool:
        if not market.has_entity(self.agent_id, Agent):
            return False
        return market.get(self.agent_id, Agent).is_homeless(market)

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[Never]:
        new_market = market.remove_entity(self.agent_id, Agent)
        return new_market, context, []
