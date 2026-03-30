from typing import TYPE_CHECKING

from core.events import Bid
from core.events.base import Event
from core.house import VacantState
from core.policies.protocol import AgentPolicy
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


class HomelessBiddingPolicy(AgentPolicy):
    @property
    def DEPENDS_ON(self) -> frozenset[Signal]:
        return frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list[Event]:
        if not agent.is_homeless(market):
            return []

        events: list[Event] = []

        for house in market.houses:
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
