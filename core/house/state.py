from typing import ClassVar, FrozenSet

from core.base import FrozenModel
from core.signals import Signal


class HouseState(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset[Signal]()


class VacantState(HouseState):
    last_update_time: float = 0.0
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset[Signal]({Signal.MARKET_RENT})


class RentedState(HouseState):
    occupant_id: str


class OwnerOccupiedState(HouseState):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset[Signal]({Signal.OWNER_MONEY})


class ConstructionState(HouseState):
    remaining_time: float


HouseStateType = VacantState | RentedState | OwnerOccupiedState | ConstructionState
