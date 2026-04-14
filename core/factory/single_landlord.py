import random

from core.engine import EventQueue, SimulationEngine
from core.entity import Agent, House
from core.entity.agent import CompositeAgentPolicy, HomelessBiddingPolicy, IncomePolicy
from core.entity.house import ConstructionState
from core.events import (
    AgentEntered,
    AgentIncomeReceived,
    AuctionClear,
    HouseAged,
    WealthTaxDeducted,
)
from core.factory.base import EngineSetup
from core.market import HousingMarket
from core.registry import SignalRegistry
from core.settings import SimulationSettings


def single_landlord_factory(settings: SimulationSettings) -> EngineSetup:
    owner_policy = IncomePolicy()
    renter_policy = CompositeAgentPolicy(
        policies=(IncomePolicy(), HomelessBiddingPolicy()),
    )

    landlord = Agent(money=100, income=0, spend_rate=0.0, policy=owner_policy)
    houses = [
        House(
            owner_id=landlord.id,
            state=ConstructionState(
                remaining_time=random.randint(
                    settings.min_construction_time, settings.max_construction_time,
                ),
            ),
        )
        for _ in range(settings.n_houses)
    ]

    market = HousingMarket.create([landlord, *houses], settings=settings)

    queue = EventQueue()
    queue = queue.push(
        AgentIncomeReceived(
            time=0,
            agent_id=landlord.id,
            amount=AgentIncomeReceived.compute_salary(landlord),
        ),
    )
    queue = queue.push(WealthTaxDeducted(time=1, agent_id=landlord.id))
    for house in market.entities_of_type(House):
        queue = queue.push(HouseAged(time=settings.aging_interval, house_id=house.id))
    queue = queue.push(AuctionClear(time=1))

    mid, mname = next(Agent.identity)
    queue = queue.push(
        AgentEntered(
            time=random.expovariate(1 / settings.migration_interval),
            agent_id=mid,
            agent_name=mname,
            policy=renter_policy,
        ),
    )

    engine = SimulationEngine(market=market, queue=queue, registry=SignalRegistry())
    return engine, landlord
