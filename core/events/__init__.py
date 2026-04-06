from core.events.aging import HouseAged, HouseDemolished
from core.events.auction import AuctionClear
from core.events.base import ApplyResult
from core.events.bid import Bid
from core.events.eviction import Evicted
from core.events.income import AgentIncomeReceived
from core.events.rent import RentCollected, RentDue, RentExpired, RentStarted

EventType = (
    AgentIncomeReceived
    | RentStarted
    | RentCollected
    | RentDue
    | RentExpired
    | Evicted
    | Bid
    | AuctionClear
    | HouseAged
    | HouseDemolished
)

__all__ = [
    "AgentIncomeReceived",
    "ApplyResult",
    "AuctionClear",
    "Bid",
    "EventType",
    "Evicted",
    "HouseAged",
    "HouseDemolished",
    "RentCollected",
    "RentDue",
    "RentExpired",
    "RentStarted",
]
