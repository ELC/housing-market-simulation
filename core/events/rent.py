from typing import TYPE_CHECKING

from core.events.base import ApplyResult, Event
from core.events.eviction import Evicted
from core.house.state import RentedState
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class RentStarted(Event):
    house_id: str
    tenant_id: str

    def invalidates(self) -> set[Signal]:
        return {Signal.HOMELESSNESS}

    def apply(self, market: "HousingMarket") -> ApplyResult:
        house = market.house_map[self.house_id]
        updated_house = house.model_copy(
            update={"state": RentedState(occupant_id=self.tenant_id)}
        )
        due = RentDue(
            time=self.time,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
            amount=house.rent_price,
        )
        new_market = market.update_entities({house.id: updated_house})
        return new_market, [due]


class RentCollected(Event):
    house_id: str
    tenant_id: str
    amount: float

    def apply(self, market: "HousingMarket") -> ApplyResult:
        house = market.house_map[self.house_id]
        owner = market.agent_map[house.owner_id]
        tenant = market.agent_map[self.tenant_id]

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
        return new_market, [next_due]


class RentDue(Event):
    house_id: str
    tenant_id: str
    amount: float

    def validate(self, market: "HousingMarket") -> bool:
        house = market.house_map[self.house_id]
        return house.occupant_id() == self.tenant_id

    def apply(self, market: "HousingMarket") -> ApplyResult:
        tenant = market.agent_map[self.tenant_id]

        if tenant.money < self.amount:
            return market, [
                Evicted(time=self.time, house_id=self.house_id, tenant_id=self.tenant_id),
            ]

        return market, [
            RentCollected(
                time=self.time,
                house_id=self.house_id,
                tenant_id=self.tenant_id,
                amount=self.amount,
            ),
        ]
