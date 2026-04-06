from __future__ import annotations

from typing import TYPE_CHECKING, ClassVar, Never, Self

from core.entity.agent import Agent
from core.entity.house import DemolishedState, House, VacantState
from core.events.base import ApplyResult, Event
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class HouseAged(Event):
    house_id: str

    def is_valid(self, market: HousingMarket) -> bool:
        house = market.get(self.house_id, House)
        return not isinstance(house.state, DemolishedState)

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[Self | HouseDemolished]:
        house = market.get(self.house_id, House)
        new_age = house.age + 1
        updated = house.model_copy(update={"age": new_age})
        new_market = market.update_entities({house.id: updated})

        if self._should_demolish(house, market):
            return new_market, context, [HouseDemolished(time=self.time, house_id=self.house_id)]

        next_event = self.model_copy(update={"time": self.time + market.settings.aging_interval})
        return new_market, context, [next_event]

    def _should_demolish(self, house: House, market: HousingMarket) -> bool:
        if not isinstance(house.state, VacantState):
            return False
        owner = market.get(house.owner_id, Agent)
        vacancy_duration = self.time - house.state.last_update_time
        return vacancy_duration >= owner.max_vacancy_periods


class HouseDemolished(Event):
    house_id: str

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[Never]:
        house = market.get(self.house_id, House)
        updated = house.model_copy(update={"state": DemolishedState()})
        new_market = market.update_entities({house.id: updated})
        return new_market, context, []
