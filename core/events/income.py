from typing import TYPE_CHECKING, Self

from core.entity.agent import Agent
from core.entity.stochastic import Stochastic
from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


_INCOME_NOISE = Stochastic(sigma=0.1)
_SPEND_RATE_NOISE = Stochastic(sigma=0.05, high=1.0)


class AgentIncomeReceived(Event):
    agent_id: str
    amount: float

    def is_valid(self, market: "HousingMarket") -> bool:
        return market.has_entity(self.agent_id, Agent)

    @staticmethod
    def compute_salary(agent: Agent) -> float:
        noised_income = _INCOME_NOISE.sample(agent.income)
        noised_spend_rate = _SPEND_RATE_NOISE.sample(agent.spend_rate)
        return noised_income * (1 - noised_spend_rate)

    def apply(self, market: "HousingMarket", context: "SimulationContext") -> ApplyResult[Self]:
        agent = market.get(self.agent_id, Agent)
        updated = agent.model_copy(update={"money": agent.money + self.amount})

        next_event = self.model_copy(update={"time": self.time + 1, "amount": self.compute_salary(agent)})
        new_market = market.update_entities({agent.id: updated})
        return new_market, context, [next_event]
