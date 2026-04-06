from __future__ import annotations

import random
from typing import TYPE_CHECKING, ClassVar, Self

from core.entity.agent import Agent
from core.entity.house import ConstructionState, DemolishedState, House, VacantState
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
        next_event = self.model_copy(update={"time": self.time + market.settings.aging_interval})

        if isinstance(house.state, ConstructionState):
            return market, context, [next_event]

        new_age = house.age + 1
        updated = house.model_copy(update={"age": new_age})
        new_market = market.update_entities({house.id: updated})

        if self._should_demolish(house, market):
            return new_market, context, [HouseDemolished(time=self.time, house_id=self.house_id)]

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

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[ReconstructionCheck]:
        house = market.get(self.house_id, House)
        owner = market.get(house.owner_id, Agent)
        updated = house.model_copy(update={"state": DemolishedState()})
        new_market = market.update_entities({house.id: updated})

        construction_time = random.randint(
            market.settings.min_construction_time,
            market.settings.max_construction_time,
        )
        cost = owner.expected_rent * construction_time

        check = ReconstructionCheck(
            time=self.time + market.settings.reconstruction_delay,
            house_id=self.house_id,
            construction_time=construction_time,
            cost=cost,
        )
        return new_market, context, [check]


class ReconstructionCheck(Event):
    house_id: str
    construction_time: int
    cost: float

    def is_valid(self, market: HousingMarket) -> bool:
        house = market.get(self.house_id, House)
        return isinstance(house.state, DemolishedState)

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[HouseRebuilt | Self]:
        owner = market.get(market.get(self.house_id, House).owner_id, Agent)
        if owner.money < self.cost:
            return market, context, [self.model_copy(update={"time": self.time + 1})]

        return market, context, [
            HouseRebuilt(
                time=self.time,
                house_id=self.house_id,
                construction_time=self.construction_time,
                cost=self.cost,
            )
        ]


class HouseRebuilt(Event):
    house_id: str
    construction_time: int
    cost: float

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[HouseAged]:
        house = market.get(self.house_id, House)
        owner = market.get(house.owner_id, Agent)

        updated_house = house.model_copy(update={
            "state": ConstructionState(remaining_time=self.construction_time),
            "age": 0,
        })
        updated_owner = owner.model_copy(update={"money": owner.money - self.cost})
        new_market = market.update_entities({house.id: updated_house, owner.id: updated_owner})

        aged = HouseAged(time=self.time + market.settings.aging_interval, house_id=self.house_id)
        return new_market, context, [aged]
