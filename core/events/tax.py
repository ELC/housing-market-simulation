from typing import TYPE_CHECKING, Self

from core.entity.agent import Agent
from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class WealthTaxDeducted(Event):
    agent_id: str
    amount: float = 0.0

    def is_valid(self, market: "HousingMarket") -> bool:
        if not market.has_entity(self.agent_id, Agent):
            return False
        agent = market.get(self.agent_id, Agent)
        return agent.money >= market.settings.min_taxable_wealth

    def apply(self, market: "HousingMarket", context: "SimulationContext") -> ApplyResult[Self]:
        agent = market.get(self.agent_id, Agent)
        new_money = max(0.0, agent.money - self.amount)
        updated = agent.model_copy(update={"money": new_money})

        next_tax = market.settings.wealth_tax_rate * new_money
        next_event = self.model_copy(update={"time": self.time + 1, "amount": next_tax})
        new_market = market.update_entities({agent.id: updated})
        return new_market, context, [next_event]
