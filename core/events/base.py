from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING

from core.base import FrozenModel
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class Event(FrozenModel, ABC):
    time: float

    def is_valid(self, market: "HousingMarket") -> bool:
        return True

    def invalidates(self) -> set[Signal]:
        return set()

    def apply(self, market: "HousingMarket") -> "ApplyResult":
        raise NotImplementedError


type ApplyResult = tuple[HousingMarket, list[Event]]
