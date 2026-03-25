from typing import TYPE_CHECKING, TypeAlias

from core.base import FrozenModel
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket

ApplyResult: TypeAlias = tuple["HousingMarket", list["EventType"]]


class Event(FrozenModel):
    time: float

    def validate(self, market: "HousingMarket") -> bool:
        return True

    def invalidates(self) -> set[Signal]:
        return set()

    def apply(self, market: "HousingMarket") -> "ApplyResult":
        raise NotImplementedError
