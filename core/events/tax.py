from typing import TYPE_CHECKING

from core.entity.agent import Agent
from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class WealthTaxDue(Event):
    agent_id: str

    def is_valid(self, market: "HousingMarket") -> bool:
        return market.has_entity(self.agent_id, Agent)

    def apply(
        self, market: "HousingMarket", context: "SimulationContext",
    ) -> ApplyResult["WealthTaxDeducted | WealthTaxDue"]:
        agent = market.get(self.agent_id, Agent)
        rate = market.settings.wealth_tax_rate
        threshold = market.settings.min_taxable_wealth

        if agent.money >= threshold:
            actual = min(rate * agent.money, max(agent.money, 0.0))
        else:
            actual = 0.0

        new_money = agent.money - actual
        updated = agent.model_copy(update={"money": new_money})
        new_market = market.update_entities({agent.id: updated})

        receipt = WealthTaxDeducted(time=self.time, agent_id=agent.id, amount=actual)
        next_due = WealthTaxDue(time=self.time + 1, agent_id=agent.id)
        return new_market, context, [receipt, next_due]


class WealthTaxDeducted(Event):
    """Receipt fact: records the actual tax deducted from an agent.

    This event is a pure record emitted by :class:`WealthTaxDue`; its
    ``apply`` is a no-op so it is faithfully preserved in the event log
    for analytics reconstruction.
    """

    agent_id: str
    amount: float

    def is_valid(self, market: "HousingMarket") -> bool:  # noqa: ARG002, PLR6301
        return True

    def apply(
        self, market: "HousingMarket", context: "SimulationContext",
    ) -> ApplyResult["WealthTaxDeducted"]:
        return market, context, []
