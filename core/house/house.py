from typing import TYPE_CHECKING, FrozenSet

from core.base import FrozenModel
from core.house.state import (
    HouseStateType,
    OwnerOccupiedState,
    RentedState,
)
from core.signals import Signal

if TYPE_CHECKING:
    from core.events import EventType
    from core.market import HousingMarket


class House(FrozenModel):
    id: str
    owner_id: str
    state: HouseStateType
    rent_price: float
    age: int

    def occupant_id(self) -> str | None:
        match self.state:
            case RentedState(occupant_id=t):
                return t
            case OwnerOccupiedState():
                return self.owner_id
            case _:
                return None

    @property
    def DEPENDS_ON(self) -> FrozenSet[Signal]:
        return self.state.DEPENDS_ON

    def decide(self, market: "HousingMarket", now: float) -> list["EventType"]:
        return []
