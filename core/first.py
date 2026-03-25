from __future__ import annotations

import heapq
import math
from collections import defaultdict
from enum import StrEnum, auto
from typing import (
    ClassVar,
    Sequence,
    Mapping,
    TypeAlias,
    Protocol,
    runtime_checkable,
    FrozenSet,
    Self,
)

from pydantic import BaseModel, Field, ConfigDict

# =========================
# Base
# =========================

class FrozenModel(BaseModel):
    model_config = ConfigDict(frozen=True, arbitrary_types_allowed=True)

ApplyResult: TypeAlias = tuple["HousingMarket", list["EventType"]]
QueueItem: TypeAlias = tuple[float, int, "EventType"]

# =========================
# Signals
# =========================

class Signal(StrEnum):
    MARKET_RENT = auto()
    HOMELESSNESS = auto()
    OWNER_MONEY = auto()

# =========================
# Signal Registry
# =========================

class SignalRegistry(FrozenModel):
    dependencies: Mapping[str, FrozenSet[str]] = Field(default_factory=dict)
    reverse_dependencies: Mapping[str, FrozenSet[str]] = Field(default_factory=dict)

    def propagate(self, invalid: set[str]) -> set[str]:
        result = set(invalid)
        queue = list(invalid)

        while queue:
            s = queue.pop()
            for dep in self.reverse_dependencies.get(s, frozenset()):
                if dep not in result:
                    result.add(dep)
                    queue.append(dep)

        return result

# =========================
# Settings
# =========================

class SimulationSettings(FrozenModel):
    base_rent: float = 10
    rent_sensitivity: float = 2.0
    tax_per_house: float = 5
    maintenance_base: float = 1
    maintenance_slope: float = 0.1
    construction_time: int = 5
    vacancy_decay_rate: float = 0.05

# =========================
# Policy
# =========================

@runtime_checkable
class AgentPolicy(Protocol):
    DEPENDS_ON: FrozenSet[Signal]

    def decide(
        self,
        agent: Agent,
        market: HousingMarket,
        now: float
    ) -> list["EventType"]:
        ...

class CompositeAgentPolicy(FrozenModel):
    policies: tuple[AgentPolicy, ...]

    @property
    def DEPENDS_ON(self) -> FrozenSet[Signal]:
        deps: set[Signal] = set()
        for p in self.policies:
            deps |= p.DEPENDS_ON
        return frozenset(deps)

    def decide(self, agent: Agent, market: HousingMarket, now: float) -> list["EventType"]:
        events: list[EventType] = []
        for p in self.policies:
            result = p.decide(agent, market, now)
            events.extend(result)
        return events

# =========================
# Policies
# =========================

class IncomePolicy(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset()

    def decide(self, agent: Agent, market: HousingMarket, now: float):
        return []

class HomelessBiddingPolicy(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset({Signal.HOMELESSNESS, Signal.MARKET_RENT})

    def decide(self, agent: Agent, market: HousingMarket, now: float):
        if not agent.is_homeless(market):
            return []

        events: list[EventType] = []

        for house in market.houses:
            if not isinstance(house.state, VacantState):
                continue

            price = agent.willingness_to_pay(house)

            if price >= house.rent_price:
                bid = Bid(
                    time=now,
                    agent_id=agent.id,
                    house_id=house.id,
                    price=price
                )
                events.append(bid)

        return events

# =========================
# Agent
# =========================

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

    def is_homeless(self, market: HousingMarket) -> bool:
        return all(h.occupant_id() != self.id for h in market.houses)

    def willingness_to_pay(self, house: House) -> float:
        return max(0.0, self.money - (self.rent_weight * house.rent_price + self.age_weight * house.age))

    def decide(self, market: HousingMarket, now: float):
        return self.policy.decide(self, market, now)

# =========================
# House
# =========================

class HouseState(FrozenModel):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset()

class VacantState(HouseState):
    last_update_time: float = 0.0
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset({Signal.MARKET_RENT})

class RentedState(HouseState):
    occupant_id: str

class OwnerOccupiedState(HouseState):
    DEPENDS_ON: ClassVar[FrozenSet[Signal]] = frozenset({Signal.OWNER_MONEY})

class ConstructionState(HouseState):
    remaining_time: float

HouseStateType = VacantState | RentedState | OwnerOccupiedState | ConstructionState

class House(FrozenModel):
    id: str
    owner_id: str
    state: HouseStateType
    rent_price: float
    age: int

    def occupant_id(self) -> str | None:
        match self.state:
            case RentedState(occupant_id=t): return t
            case OwnerOccupiedState(): return self.owner_id
            case _: return None

    @property
    def DEPENDS_ON(self) -> FrozenSet[Signal]:
        return self.state.DEPENDS_ON

    def decide(self, market: HousingMarket, now: float) -> list[EventType]:
        return []

# =========================
# Events
# =========================

class Event(FrozenModel):
    time: float

    def validate(self, market: HousingMarket) -> bool:
        return True

    def invalidates(self) -> set[Signal]:
        return set()

    def apply(self, market: HousingMarket) -> ApplyResult:
        raise NotImplementedError

class AgentIncomeReceived(Event):
    agent_id: str

    def apply(self, market: HousingMarket) -> ApplyResult:
        agent = market.agent_map[self.agent_id]
        updated = agent.model_copy(update={"money": agent.money + agent.income * (1 - agent.spend_rate)})
        next_event = self.model_copy(update={"time": self.time + 1})
        auction = AuctionClear(time=self.time)
        new_market = market.update_agents({agent.id: updated})
        return new_market, [next_event, auction]

class RentStarted(Event):
    house_id: str
    tenant_id: str

    def invalidates(self) -> set[Signal]:
        return {Signal.HOMELESSNESS}

    def apply(self, market: HousingMarket) -> ApplyResult:
        house = market.house_map[self.house_id]
        updated_house = house.model_copy(update={"state": RentedState(occupant_id=self.tenant_id)})
        rent_event = RentPaid(
            time=self.time,
            house_id=self.house_id,
            tenant_id=self.tenant_id,
            amount=house.rent_price
        )
        new_market = market.update_houses({house.id: updated_house})
        return new_market, [rent_event]

class RentPaid(Event):
    house_id: str
    tenant_id: str
    amount: float

    def validate(self, market: HousingMarket) -> bool:
        house = market.house_map[self.house_id]
        return house.occupant_id() == self.tenant_id

    def apply(self, market: HousingMarket) -> ApplyResult:
        house = market.house_map[self.house_id]
        owner = market.agent_map[house.owner_id]
        tenant = market.agent_map[self.tenant_id]

        if tenant.money < self.amount:
            eviction = Evicted(time=self.time, house_id=self.house_id, tenant_id=self.tenant_id)
            return market, [eviction]

        updated_agents = {
            tenant.id: tenant.model_copy(update={"money": tenant.money - self.amount}),
            owner.id: owner.model_copy(update={"money": owner.money + self.amount}),
        }

        new_market = market.update_agents(updated_agents)
        next_rent = self.model_copy(update={"time": self.time + 1})
        return new_market, [next_rent]

class Evicted(Event):
    house_id: str
    tenant_id: str

    def invalidates(self) -> set[Signal]:
        return {Signal.HOMELESSNESS, Signal.MARKET_RENT}

    def apply(self, market: HousingMarket) -> ApplyResult:
        house = market.house_map[self.house_id]
        updated_house = house.model_copy(update={"state": VacantState(last_update_time=self.time)})
        new_market = market.update_houses({house.id: updated_house})
        return new_market, []

class Bid(Event):
    agent_id: str
    house_id: str
    price: float

    def apply(self, market: HousingMarket) -> ApplyResult:
        new_bids = (*market.pending_bids, self)
        new_market = market.model_copy(update={"pending_bids": new_bids})
        return new_market, []

class AuctionClear(Event):

    def invalidates(self) -> set[Signal]:
        return {Signal.MARKET_RENT, Signal.HOMELESSNESS}

    def apply(self, market: HousingMarket) -> ApplyResult:
        bids_by_house = self._group_bids(market)
        updated_houses = {}
        events: list[EventType] = []
        matched: set[str] = {occ for h in market.houses if (occ := h.occupant_id())}

        for house in market.houses:
            match house.state:
                case VacantState():
                    result = self._handle_vacant(house, bids_by_house, market, matched)
                    updated_houses[house.id] = result.house
                    events.extend(result.events)

                case ConstructionState():
                    updated_houses[house.id] = self._handle_construction(house)

        new_market = market.model_copy(
            update={
                "pending_bids": (),
                "houses": tuple(updated_houses.get(h.id, h) for h in market.houses),
            }
        )

        return new_market, events

    def _group_bids(self, market: HousingMarket):
        grouped: dict[str, list[Bid]] = defaultdict(list)
        for b in market.pending_bids:
            grouped[b.house_id].append(b)
        return grouped

    def _handle_vacant(self, house: House, bids_by_house, market: HousingMarket, matched: set[str]):
        bids = bids_by_house.get(house.id, [])
        valid = [b for b in bids if b.price >= house.rent_price and b.agent_id not in matched]

        if not valid:
            decay = math.exp(-market.settings.vacancy_decay_rate)
            new_price = house.rent_price * decay
            updated = house.model_copy(update={"rent_price": new_price})
            return _AuctionResult(updated, [])

        sorted_bids = sorted(valid, key=lambda b: b.price, reverse=True)
        winner = sorted_bids[0]

        matched.add(winner.agent_id)
        rent_event = RentStarted(time=self.time, house_id=house.id, tenant_id=winner.agent_id)

        return _AuctionResult(house, [rent_event])

    def _handle_construction(self, house: House):
        state = house.state
        if state.remaining_time <= 0:
            return house.model_copy(update={"state": VacantState(last_update_time=self.time)})
        return house.model_copy(update={"state": ConstructionState(remaining_time=state.remaining_time - 1)})

class _AuctionResult:
    def __init__(self, house: House, events: list["EventType"]):
        self.house = house
        self.events = events

EventType = AgentIncomeReceived | RentStarted | RentPaid | Evicted | Bid | AuctionClear

# =========================
# Market
# =========================

class HousingMarket(FrozenModel):
    agents: Sequence[Agent]
    houses: Sequence[House]
    settings: SimulationSettings
    pending_bids: Sequence[Bid] = ()

    @property
    def agent_map(self):
        return {a.id: a for a in self.agents}

    @property
    def house_map(self):
        return {h.id: h for h in self.houses}

    def update_agents(self, updates: dict[str, Agent]) -> Self:
        updated = tuple(updates.get(a.id, a) for a in self.agents)
        return self.model_copy(update={"agents": updated})

    def update_houses(self, updates: dict[str, House]) -> Self:
        updated = tuple(updates.get(h.id, h) for h in self.houses)
        return self.model_copy(update={"houses": updated})

# =========================
# Queue + Engine
# =========================

class EventQueue(FrozenModel):
    events: Sequence[QueueItem] = ()
    counter: int = 0

    def push(self, e: EventType) -> Self:
        items = list(self.events)
        heapq.heappush(items, (e.time, self.counter, e))
        return self.model_copy(update={"events": tuple(items), "counter": self.counter + 1})

    def pop(self) -> tuple[EventType, Self]:
        items = list(self.events)
        _, _, event = heapq.heappop(items)
        return event, self.model_copy(update={"events": tuple(items)})

class SimulationEngine(FrozenModel):
    market: HousingMarket
    queue: EventQueue
    registry: SignalRegistry
    now: float = 0.0
    event_log: Sequence[EventType] = ()

    def step(self):
        event, queue = self.queue.pop()

        if not event.validate(self.market):
            return self.model_copy(update={"queue": queue, "now": event.time})

        market, emitted = event.apply(self.market)
        invalid = self.registry.propagate(event.invalidates())

        new_events: list[EventType] = list(emitted)

        for entity in (*market.agents, *market.houses):
            if entity.DEPENDS_ON & invalid:
                decisions = entity.decide(market, event.time)
                new_events.extend(decisions)

        for e in new_events:
            queue = queue.push(e)

        return self.model_copy(
            update={
                "market": market,
                "queue": queue,
                "now": event.time,
                "event_log": (*self.event_log, event),
            }
        )
