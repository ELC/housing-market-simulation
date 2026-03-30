from typing import TYPE_CHECKING

from core.entity.agent.protocol import AgentPolicy
from core.entity.house import House, VacantState
from core.events import Bid, EventType
from core.signals import Signal

if TYPE_CHECKING:
    from core.entity.agent.agent import Agent
    from core.market import HousingMarket


class HomelessBiddingPolicy(AgentPolicy):
    @property
    def depends_on(self) -> frozenset[Signal]:
        return frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list[EventType]:
        if not agent.is_homeless(market):
            return []

        events = list[EventType]()

        for house in market.entities_of_type(House):
            if not isinstance(house.state, VacantState):
                continue

            price = agent.willingness_to_pay(house)

            if price < house.rent_price:
                continue

            bid = Bid(
                time=now,
                agent_id=agent.id,
                house_id=house.id,
                price=price,
            )
            events.append(bid)

        return events
