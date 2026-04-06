import math
import random
from typing import TYPE_CHECKING, ClassVar

from pydantic import Field

from core.entity.agent.protocol import AgentPolicy
from core.entity.entity import Entity
from core.entity.house import House
from core.entity.identity import EntityIdentity
from core.signals import Signal

if TYPE_CHECKING:
    from core.events import EventType
    from core.market import HousingMarket


class Agent(Entity):
    identity: ClassVar[EntityIdentity] = EntityIdentity(provider="first_name")

    money: float = Field(default_factory=lambda: random.lognormvariate(math.log(3), 1))
    income: float = Field(default_factory=lambda: random.lognormvariate(math.log(8), 0.3))
    spend_rate: float = Field(default_factory=lambda: random.uniform(0.1, 0.9))
    policy: AgentPolicy

    age_weight: float = Field(default_factory=lambda: random.uniform(0.05, 0.2))
    expected_rent: float = Field(default_factory=lambda: random.lognormvariate(math.log(5), 0.3))
    horizon: float = Field(default_factory=lambda: random.uniform(10, 30))
    max_vacancy_periods: float = Field(default_factory=lambda: random.uniform(50, 100))
    max_homeless_periods: float = Field(default_factory=lambda: random.uniform(20, 60))

    @property
    def depends_on(self) -> frozenset[Signal]:
        return self.policy.depends_on

    @property
    def expected_savings(self) -> float:
        return self.income * (1 - self.spend_rate)

    def is_homeless(self, market: "HousingMarket") -> bool:
        return all(h.occupant_id() != self.id for h in market.entities_of_type(House))

    def willingness_to_pay(self, house: House) -> float:
        affordable_rent = self.expected_savings + self.money / self.horizon
        return max(0.0, affordable_rent - self.age_weight * house.age)

    def decide(self, market: "HousingMarket", now: float) -> list["EventType"]:
        return self.policy.decide(self, market, now)
