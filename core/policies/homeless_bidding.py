from typing import TYPE_CHECKING, ClassVar, FrozenSet

from core.base import FrozenModel
from core.events.bid import Bid
from core.house.state import VacantState
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


class HomelessBiddingPolicy(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset(
        {Signal.HOMELESSNESS, Signal.MARKET_RENT}
    )

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list:
        if not agent.is_homeless(market):
            return []

        events: list = []

        for house in market.houses:
            if not isinstance(house.state, VacantState):
                continue

            price = agent.willingness_to_pay(house)

            if price >= house.rent_price:
                bid = Bid(
                    time=now,
                    agent_id=agent.id,
                    house_id=house.id,
                    price=price,
                )
                events.append(bid)

        return events
