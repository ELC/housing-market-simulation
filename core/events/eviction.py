from typing import TYPE_CHECKING, Never

from core.events.base import ApplyResult, Event
from core.house import VacantState
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class Evicted(Event):
    house_id: str
    tenant_id: str

    def invalidates(self) -> set[Signal]:
        return {Signal.HOMELESSNESS, Signal.MARKET_RENT}

    def apply(self, market: "HousingMarket") -> ApplyResult[Never]:
        house = market.house_map[self.house_id]
        updated_house = house.model_copy(update={"state": VacantState(last_update_time=self.time)})
        new_market = market.update_entities({house.id: updated_house})
        return new_market, []
