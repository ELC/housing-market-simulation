from typing import TYPE_CHECKING, Protocol, runtime_checkable

from core.events.base import Event
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


@runtime_checkable
class Entity(Protocol):
    @property
    def id(self) -> str: ...

    @property
    def DEPENDS_ON(self) -> frozenset[Signal]: ...

    def decide(self, market: "HousingMarket", now: float) -> list[Event]: ...
