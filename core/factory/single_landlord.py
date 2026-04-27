import random

from core.engine import EventQueue, SimulationEngine
from core.entity import Agent
from core.entity.agent import CompositeAgentPolicy, HomelessBiddingPolicy, IncomePolicy
from core.events import AgentEntered, AuctionClear
from core.events.landlord_migration import LandlordArrival, LandlordEntered
from core.market import HousingMarket
from core.registry import SignalRegistry
from core.settings import SimulationSettings


def single_landlord_factory(settings: SimulationSettings) -> SimulationEngine:
    renter_policy = CompositeAgentPolicy(
        policies=(IncomePolicy(), HomelessBiddingPolicy()),
    )

    market = HousingMarket.create([], settings=settings)

    queue = EventQueue()
    for _ in range(settings.n_initial_landlords):
        landlord_id, landlord_name = next(Agent.identity)
        queue = queue.push(LandlordEntered(
            time=0,
            agent_id=landlord_id,
            agent_name=landlord_name,
            initial_money=settings.landlord_seed_capital,
        ))

    queue = queue.push(AuctionClear(time=1))

    renter_id, renter_name = next(Agent.identity)
    queue = queue.push(AgentEntered(
        time=random.expovariate(1 / settings.migration_interval),
        agent_id=renter_id,
        agent_name=renter_name,
        policy=renter_policy,
    ))

    queue = queue.push(LandlordArrival(
        time=random.expovariate(1 / settings.landlord_migration_interval),
    ))

    return SimulationEngine(market=market, queue=queue, registry=SignalRegistry())
