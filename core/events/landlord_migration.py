from __future__ import annotations

import random
from typing import TYPE_CHECKING, ClassVar, Never, Self

from pydantic import Field

from core.entity.agent import Agent
from core.entity.agent.protocol import PassiveLandlordPolicy
from core.entity.house import House
from core.events.base import ApplyResult, Event
from core.events.construction import ConstructionCheck
from core.events.tax import WealthTaxDue
from core.signals import Signal

if TYPE_CHECKING:
    from core.context import SimulationContext
    from core.market import HousingMarket


class LandlordEntered(Event):
    agent_id: str
    agent_name: str
    initial_money: float = Field(gt=0)

    invalidates: ClassVar[frozenset[Signal]] = frozenset()

    def apply(
        self, market: HousingMarket, context: SimulationContext,
    ) -> ApplyResult[WealthTaxDue | ConstructionCheck | "LandlordLeft"]:
        max_idle = random.uniform(30, 100)
        landlord = Agent(
            id=self.agent_id,
            name=self.agent_name,
            money=self.initial_money,
            income=0.0,
            spend_rate=0.0,
            policy=PassiveLandlordPolicy(),
            idle_since=self.time,
            max_idle_periods=max_idle,
        )
        new_market = market.update_entities({landlord.id: landlord})

        tax = WealthTaxDue(time=self.time + 1, agent_id=landlord.id)
        check = ConstructionCheck(time=self.time, landlord_id=landlord.id)
        left = LandlordLeft(
            time=self.time + max_idle,
            agent_id=landlord.id,
            idle_since=self.time,
        )

        return new_market, context, [tax, check, left]


class LandlordArrival(Event):
    """Migration tick: emits one `LandlordEntered` and reschedules itself."""

    invalidates: ClassVar[frozenset[Signal]] = frozenset()

    def apply(
        self, market: HousingMarket, context: SimulationContext,
    ) -> ApplyResult[LandlordEntered | Self]:
        agent_id, agent_name = next(Agent.identity)
        entry = LandlordEntered(
            time=self.time,
            agent_id=agent_id,
            agent_name=agent_name,
            initial_money=market.settings.landlord_seed_capital,
        )

        next_interval = random.expovariate(1 / market.settings.landlord_migration_interval)
        next_tick = self.model_copy(update={"time": self.time + next_interval})

        return market, context, [entry, next_tick]


class LandlordLeft(Event):
    agent_id: str
    idle_since: float

    invalidates: ClassVar[frozenset[Signal]] = frozenset()

    def is_valid(self, market: HousingMarket) -> bool:
        if not market.has_entity(self.agent_id, Agent):
            return False
        agent = market.get(self.agent_id, Agent)
        has_houses = any(h.owner_id == self.agent_id for h in market.entities_of_type(House))
        return not has_houses and agent.idle_since == self.idle_since

    def apply(
        self, market: HousingMarket, context: SimulationContext,
    ) -> ApplyResult[Never]:
        new_market = market.remove_entity(self.agent_id, Agent)
        return new_market, context, []
