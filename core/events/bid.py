from typing import TYPE_CHECKING, Never

from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.market import HousingMarket


class Bid(Event):
    agent_id: str
    house_id: str
    price: float

    def apply(self, market: "HousingMarket") -> ApplyResult[Never]:
        new_bids = (*market.pending_bids, self)
        new_market = market.model_copy(update={"pending_bids": new_bids})
        return new_market, []
