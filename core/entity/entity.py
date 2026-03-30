from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, ClassVar

from pydantic import model_validator

from core.base import FrozenModel
from core.entity.identity import EntityIdentity
from core.signals import Signal

if TYPE_CHECKING:
    from core.events import EventType
    from core.market import HousingMarket


class Entity(FrozenModel, ABC):
    id: str
    name: str
    identity: ClassVar[EntityIdentity]

    @model_validator(mode="before")
    @classmethod
    def _auto_identity(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "id" in data:
            return data

        eid, ename = next(cls.identity)
        data.setdefault("id", eid)
        data.setdefault("name", ename)
        return data

    @property
    @abstractmethod
    def DEPENDS_ON(self) -> frozenset[Signal]: ...

    @abstractmethod
    def decide(self, market: "HousingMarket", now: float) -> list["EventType"]: ...
