from __future__ import annotations

import math
from typing import TYPE_CHECKING, ClassVar, Self

from core.entity.agent import Agent
from core.entity.house import ConstructionState, House, VacantState
from core.entity.identity import EntityIdentity
from core.events.base import ApplyResult, Event
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class ConstructionCheck(Event):
    landlord_id: str

    def is_valid(self, market: HousingMarket) -> bool:
        return market.has_entity(self.landlord_id, Agent)

    def apply(
        self, market: HousingMarket, context: SimulationContext,
    ) -> ApplyResult[Self | HouseBuilt]:
        landlord = market.get(self.landlord_id, Agent)
        next_check = self.model_copy(
            update={"time": self.time + market.settings.construction_check_interval},
        )

        houses = market.entities_of_type(House)
        active = [h for h in houses if not isinstance(h.state, ConstructionState)]
        n_active = len(active)
        n_vacant = sum(1 for h in active if isinstance(h.state, VacantState))
        vacancy_rate = n_vacant / n_active if n_active > 0 else 0.0

        avg_age = sum(h.age for h in active) / n_active if n_active else 0.0
        construction_cost = landlord.expected_rent * (
            market.settings.min_construction_time + market.settings.max_construction_time
        ) / 2
        expected_revenue = landlord.expected_rent * (1.0 - vacancy_rate)
        expected_cost = construction_cost * (
            market.settings.maintenance_base + market.settings.maintenance_slope * avg_age
        )
        expected_profit = expected_revenue - expected_cost

        if expected_profit <= 0 or landlord.money < construction_cost:
            return market, context, [next_check]

        house_id, house_name = next(House.identity)
        return market, context, [
            HouseBuilt(
                time=self.time,
                landlord_id=self.landlord_id,
                house_id=house_id,
                house_name=house_name,
                cost=construction_cost,
            ),
            next_check,
        ]


class HouseBuilt(Event):
    landlord_id: str
    house_id: str
    house_name: str
    cost: float

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def is_valid(self, market: HousingMarket) -> bool:
        return market.has_entity(self.landlord_id, Agent)

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[Self]:
        landlord = market.get(self.landlord_id, Agent)
        construction_time = max(1, math.ceil(self.cost / market.settings.construction_speed))

        house = House(
            id=self.house_id,
            name=self.house_name,
            owner_id=self.landlord_id,
            state=ConstructionState(remaining_time=construction_time),
            rent_price=landlord.expected_rent,
            age=0,
            construction_cost=self.cost,
        )

        updated_landlord = landlord.model_copy(update={
            "money": landlord.money - self.cost,
            "idle_since": None,
        })
        new_market = market.update_entities({house.id: house, landlord.id: updated_landlord})

        return new_market, context, []
