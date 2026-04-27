from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Self

from core.entity.agent import Agent
from core.entity.house import DemolishedState, House, VacantState
from core.events.base import ApplyResult, Event
from core.events.landlord_migration import LandlordLeft
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class HouseAged(Event):
    house_id: str

    def is_valid(self, market: HousingMarket) -> bool:
        return market.has_entity(self.house_id, House)

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[Self | "HouseDemolished"]:
        house = market.get(self.house_id, House)
        next_event = self.model_copy(update={"time": self.time + market.settings.aging_interval})

        new_age = house.age + 1
        updated = house.model_copy(update={"age": new_age})
        new_market = market.update_entities({house.id: updated})

        if self._should_demolish(house, market):
            return new_market, context, [
                HouseDemolished(time=self.time, house_id=self.house_id, age=new_age),
            ]

        return new_market, context, [next_event]

    def _should_demolish(self, house: House, market: HousingMarket) -> bool:
        if not isinstance(house.state, VacantState):
            return False
        if not market.has_entity(house.owner_id, Agent):
            return True
        owner = market.get(house.owner_id, Agent)
        vacancy_duration = self.time - house.state.last_update_time
        return vacancy_duration >= owner.max_vacancy_periods


class HouseDemolished(Event):
    house_id: str
    age: int = 0

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def is_valid(self, market: HousingMarket) -> bool:
        return market.has_entity(self.house_id, House)

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[LandlordLeft]:
        house = market.get(self.house_id, House)
        new_market = market.remove_entity(self.house_id, House)

        if not market.has_entity(house.owner_id, Agent):
            return new_market, context, []

        owner = market.get(house.owner_id, Agent)
        remaining_houses = [
            h for h in new_market.entities_of_type(House)
            if h.owner_id == owner.id and not isinstance(h.state, DemolishedState)
        ]

        events: list[LandlordLeft] = []
        if remaining_houses:
            return new_market, context, events

        updated_owner = owner.model_copy(update={"idle_since": self.time})
        new_market = new_market.update_entities({owner.id: updated_owner})
        events.append(LandlordLeft(
            time=self.time + owner.max_idle_periods,
            agent_id=owner.id,
            idle_since=self.time,
        ))

        return new_market, context, events
   
