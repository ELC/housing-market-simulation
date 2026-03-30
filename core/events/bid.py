from typing import TYPE_CHECKING, Never

from core.events.base import ApplyResult, Event

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class Bid(Event):
    agent_id: str
    house_id: str
    price: float

    def apply(self, market: "HousingMarket", context: "SimulationContext") -> ApplyResult[Never]:
        new_context = context.model_copy(update={"pending_bids": (*context.pending_bids, self)})
        return market, new_context, []
