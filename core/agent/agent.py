from typing import TYPE_CHECKING, FrozenSet

from core.base import FrozenModel
from core.house import House
from core.policies import AgentPolicy
from core.signals import Signal

if TYPE_CHECKING:
    from core.market import HousingMarket


class Agent(FrozenModel):
    id: str
    money: float
    income: float
    spend_rate: float
    policy: AgentPolicy

    rent_weight: float = 1.0
    age_weight: float = 0.1

    @property
    def DEPENDS_ON(self) -> FrozenSet[Signal]:
        return self.policy.DEPENDS_ON

    def is_homeless(self, market: "HousingMarket") -> bool:
        return all(h.occupant_id() != self.id for h in market.houses)

    def willingness_to_pay(self, house: "House") -> float:
        return max(
            0.0,
            self.money
            - (self.rent_weight * house.rent_price + self.age_weight * house.age),
        )

    def decide(self, market: "HousingMarket", now: float) -> list:
        return self.policy.decide(self, market, now)
