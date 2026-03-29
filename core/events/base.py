from abc import ABC
from typing import TYPE_CHECKING, Sequence, TypeAlias

from core.base import FrozenModel
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class Event(FrozenModel, ABC):
    time: float

    def validate(self, market: "HousingMarket") -> bool:
        return True

    def invalidates(self) -> set[Signal]:
        return set()

    def apply(self, market: "HousingMarket") -> "ApplyResult":
        raise NotImplementedError


ApplyResult: TypeAlias = tuple["HousingMarket", Sequence[Event]]
