from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, ClassVar

from core.base import FrozenModel

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket
    from core.signals import Signal


type ApplyResult[T: Event] = tuple[HousingMarket, SimulationContext, Sequence[T]]


class Event(FrozenModel, ABC):
    time: float
    invalidates: ClassVar[frozenset[Signal]] = frozenset()

    def is_valid(self, market: HousingMarket) -> bool:
        return True

    @abstractmethod
    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[Event]: ...
