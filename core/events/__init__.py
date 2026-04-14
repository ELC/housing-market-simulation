from core.events.aging import HouseAged, HouseDemolished, HouseRebuilt, ReconstructionCheck
from core.events.auction import AuctionClear
from core.events.base import ApplyResult
from core.events.bid import Bid
from core.events.eviction import Evicted
from core.events.income import AgentIncomeReceived
from core.events.migration import AgentEntered, AgentLeft
from core.events.rent import RentCollected, RentDue, RentExpired, RentStarted
from core.events.tax import WealthTaxDeducted

EventType = (
    AgentIncomeReceived
    | AgentEntered
    | AgentLeft
    | RentStarted
    | RentCollected
    | RentDue
    | RentExpired
    | Evicted
    | Bid
    | AuctionClear
    | HouseAged
    | HouseDemolished
    | ReconstructionCheck
    | HouseRebuilt
    | WealthTaxDeducted
)

__all__ = [
    "AgentEntered",
    "AgentIncomeReceived",
    "AgentLeft",
    "ApplyResult",
    "AuctionClear",
    "Bid",
    "EventType",
    "Evicted",
    "HouseAged",
    "HouseDemolished",
    "HouseRebuilt",
    "ReconstructionCheck",
    "RentCollected",
    "RentDue",
    "RentExpired",
    "RentStarted",
    "WealthTaxDeducted",
]
