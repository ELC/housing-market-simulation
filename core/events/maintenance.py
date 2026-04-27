from __future__ import annotations

from typing import TYPE_CHECKING, Self

from core.entity.agent import Agent
from core.entity.house import (
    ConstructionState,
    DemolishedState,
    House,
    RentedState,
)
from core.events.aging import HouseDemolished
from core.events.base import ApplyResult, Event
from core.events.eviction import Evicted

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class MaintenanceDue(Event):
    house_id: str

    def is_valid(self, market: HousingMarket) -> bool:
        if not market.has_entity(self.house_id, House):
            return False

        house = market.get(self.house_id, House)
        if not market.has_entity(house.owner_id, Agent):
            return False

        return not isinstance(house.state, DemolishedState)

    def apply(
        self,
        market: HousingMarket,
        context: SimulationContext,
    ) -> ApplyResult[Self | Evicted | HouseDemolished]:
        house = market.get(self.house_id, House)

        if isinstance(house.state, ConstructionState):
            return market, context, [self.model_copy(update={"time": self.time + 1})]

        cost = house.construction_cost * (
            market.settings.maintenance_base + market.settings.maintenance_slope * house.age
        )

        owner = market.get(house.owner_id, Agent)

        # An owner who can't afford upkeep defaults on the property: the
        # house is demolished (and its tenant evicted) instead of driving
        # the owner into negative wealth.
        if owner.money < cost:
            demolition_events: list[Evicted | HouseDemolished] = []
            if isinstance(house.state, RentedState):
                demolition_events.append(
                    Evicted(time=self.time, house_id=house.id, tenant_id=house.state.occupant_id),
                )
            demolition_events.append(
                HouseDemolished(time=self.time, house_id=house.id, age=house.age),
            )
            return market, context, demolition_events

        updated_owner = owner.model_copy(update={"money": owner.money - cost})
        new_market = market.update_entities({owner.id: updated_owner})

        next_event = self.model_copy(update={"time": self.time + 1})

        if cost > house.rent_price:
            if isinstance(house.state, RentedState):
                return (
                    new_market,
                    context,
                    [
                        Evicted(time=self.time, house_id=house.id, tenant_id=house.state.occupant_id),
                        HouseDemolished(time=self.time, house_id=house.id, age=house.age),
                    ],
                )
            return (
                new_market,
                context,
                [
                    HouseDemolished(time=self.time, house_id=house.id, age=house.age),
                ],
            )

        return new_market, context, [next_event]
