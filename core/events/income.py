from typing import TYPE_CHECKING, Self

from core.events.auction import AuctionClear
from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.market import HousingMarket


class AgentIncomeReceived(Event):
    agent_id: str
    amount: float

    def apply(self, market: "HousingMarket") -> ApplyResult[Self | AuctionClear]:
        agent = market.agent_map[self.agent_id]
        updated = agent.model_copy(update={"money": agent.money + self.amount})
        next_event = self.model_copy(update={"time": self.time + 1})
        auction = AuctionClear(time=self.time)
        new_market = market.update_entities({agent.id: updated})
        return new_market, [next_event, auction]
