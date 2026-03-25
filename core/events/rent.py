from typing import TYPE_CHECKING

from core.events.base import ApplyResult, Event
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
        rent_event = RentPaid(
            time=self.time,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
            amount=house.rent_price,
        )
        new_market = market.update_entities({house.id: updated_house})
        return new_market, [rent_event]


class RentPaid(Event):
    house_id: str
    tenant_id: str
    amount: float

    def validate(self, market: "HousingMarket") -> bool:
        house = market.house_map[self.house_id]
        return house.occupant_id() == self.tenant_id

    def apply(self, market: "HousingMarket") -> ApplyResult:
        from core.events.eviction import Evicted

        house = market.house_map[self.house_id]
        owner = market.agent_map[house.owner_id]
        tenant = market.agent_map[self.tenant_id]

        if tenant.money < self.amount:
            eviction = Evicted(
                time=self.time, house_id=self.house_id, tenant_id=self.tenant_id
            )
            return market, [eviction]

        updated_agents = {
            tenant.id: tenant.model_copy(update={"money": tenant.money - self.amount}),
            owner.id: owner.model_copy(update={"money": owner.money + self.amount}),
        }

        new_market = market.update_entities(updated_agents)
        next_rent = self.model_copy(update={"time": self.time + 1})
        return new_market, [next_rent]
