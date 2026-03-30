from __future__ import annotations

from abc import ABC
from collections.abc import Sequence
from typing import TYPE_CHECKING

from core.base import FrozenModel

if TYPE_CHECKING:
    from core.market import HousingMarket
    from core.signals import Signal


type ApplyResult[T: Event] = tuple[HousingMarket, Sequence[T]]


class Event(FrozenModel, ABC):
    time: float

    def is_valid(self, market: HousingMarket) -> bool:
        return True

    def invalidates(self) -> set[Signal]:
        return set()

    def apply(self, market: HousingMarket) -> ApplyResult[Event]:
        raise NotImplementedError
