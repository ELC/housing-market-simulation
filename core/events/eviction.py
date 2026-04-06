from typing import TYPE_CHECKING, ClassVar, Never

from core.entity.house import House, VacantState
from core.events.base import ApplyResult, Event
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class Evicted(Event):
    house_id: str
    tenant_id: str

    invalidates: ClassVar[frozenset[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def apply(self, market: "HousingMarket", context: "SimulationContext") -> ApplyResult[Never]:
        house = market.get(self.house_id, House)
        updated_house = house.model_copy(update={"state": VacantState(last_update_time=self.time)})
        new_market = market.update_entities({house.id: updated_house})
        return new_market, context, []
