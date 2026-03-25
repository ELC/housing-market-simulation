from typing import TYPE_CHECKING, ClassVar, FrozenSet

from core.base import FrozenModel
from core.signals import Signal

if TYPE_CHECKING:
    from core.agent import Agent
    from core.market import HousingMarket


class IncomePolicy(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset()

    def decide(self, agent: "Agent", market: "HousingMarket", now: float) -> list:
        return []
