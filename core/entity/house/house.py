from typing import TYPE_CHECKING, ClassVar

from core.entity.entity import Entity
from core.entity.house.state import (
    HouseStateType,
    OwnerOccupiedState,
    RentedState,
)
from core.entity.identity import EntityIdentity
from core.signals import Signal

if TYPE_CHECKING:
    from core.events import EventType
    from core.market import HousingMarket


class House(Entity):
    identity: ClassVar[EntityIdentity] = EntityIdentity(provider="street_address")

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
    def depends_on(self) -> frozenset[Signal]:
        return self.state.depends_on

    def decide(self, market: "HousingMarket", now: float) -> list["EventType"]:
        return []
