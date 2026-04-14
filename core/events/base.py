from __future__ import annotations

import re
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import TYPE_CHECKING, ClassVar

from core.base import FrozenModel

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket
    from core.signals import Signal

_CAMEL_RE = re.compile(r"(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])")

type ApplyResult[T: Event] = tuple[HousingMarket, SimulationContext, Sequence[T]]


class Event(FrozenModel, ABC):
    time: float
    invalidates: ClassVar[frozenset[Signal]] = frozenset()

    @classmethod
    def event_name(cls) -> str:
        return _CAMEL_RE.sub("_", cls.__name__).lower()

    def is_valid(self, market: HousingMarket) -> bool:
        return True

    @abstractmethod
    def apply(
        self, market: HousingMarket, context: SimulationContext
    ) -> ApplyResult[Event]: ...
