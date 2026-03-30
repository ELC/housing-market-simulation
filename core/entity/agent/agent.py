from typing import TYPE_CHECKING, ClassVar

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

    money: float
    income: float
    spend_rate: float
    policy: AgentPolicy

    rent_weight: float = 1.0
    age_weight: float = 0.1

    @property
    def depends_on(self) -> frozenset[Signal]:
        return self.policy.depends_on

    def is_homeless(self, market: "HousingMarket") -> bool:
        return all(h.occupant_id() != self.id for h in market.entities_of_type(House))

    def willingness_to_pay(self, house: House) -> float:
        return max(
            0.0,
            self.money - (self.rent_weight * house.rent_price + self.age_weight * house.age),
        )

    def decide(self, market: "HousingMarket", now: float) -> list["EventType"]:
        return self.policy.decide(self, market, now)
