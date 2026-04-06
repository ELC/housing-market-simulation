from __future__ import annotations

import random
from typing import TYPE_CHECKING, ClassVar, Never

from core.entity.agent import Agent
from core.entity.house import House, RentedState, VacantState
from core.events.base import ApplyResult, Event
from core.events.eviction import Evicted
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class RentStarted(Event):
    house_id: str
    tenant_id: str

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS})

    def is_valid(self, market: HousingMarket) -> bool:
        house = market.get(self.house_id, House)
        return isinstance(house.state, VacantState)

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[RentDue | RentExpired]:
        house = market.get(self.house_id, House)
        updated_house = house.model_copy(update={"state": RentedState(occupant_id=self.tenant_id)})
        due = RentDue(
            time=self.time,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
            amount=house.rent_price,
        )
        lease_duration = random.randint(
            market.settings.min_lease_duration,
            market.settings.max_lease_duration,
        )
        expiry = RentExpired(
            time=self.time + lease_duration,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
        )
        new_market = market.update_entities({house.id: updated_house})
        return new_market, context, [due, expiry]


class RentExpired(Event):
    house_id: str
    tenant_id: str

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def is_valid(self, market: HousingMarket) -> bool:
        house = market.get(self.house_id, House)
        return house.occupant_id() == self.tenant_id

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[Never]:
        house = market.get(self.house_id, House)
        updated_house = house.model_copy(update={"state": VacantState(last_update_time=self.time)})
        new_market = market.update_entities({house.id: updated_house})
        return new_market, context, []


class RentCollected(Event):
    house_id: str
    tenant_id: str
    amount: float

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[RentDue]:
        house = market.get(self.house_id, House)
        owner = market.get(house.owner_id, Agent)
        tenant = market.get(self.tenant_id, Agent)

        updated = {
            tenant.id: tenant.model_copy(update={"money": tenant.money - self.amount}),
            owner.id: owner.model_copy(update={"money": owner.money + self.amount}),
        }
        new_market = market.update_entities(updated)
        next_due = RentDue(
            time=self.time + 1,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
            amount=self.amount,
        )
        return new_market, context, [next_due]


class RentDue(Event):
    house_id: str
    tenant_id: str
    amount: float

    def is_valid(self, market: HousingMarket) -> bool:
        house = market.get(self.house_id, House)
        return house.occupant_id() == self.tenant_id

    def apply(self, market: HousingMarket, context: SimulationContext) -> ApplyResult[RentCollected | Evicted]:
        tenant = market.get(self.tenant_id, Agent)

        if tenant.money < self.amount:
            return (
                market,
                context,
                [
                    Evicted(time=self.time, house_id=self.house_id, tenant_id=self.tenant_id),
                ],
            )

        return (
            market,
            context,
            [
                RentCollected(
                    time=self.time,
                    house_id=self.house_id,
                    tenant_id=self.tenant_id,
                    amount=self.amount,
                ),
            ],
        )
