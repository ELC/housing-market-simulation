from core.events.aging import HouseAged, HouseDemolished
from core.events.auction import AuctionClear
from core.events.base import ApplyResult
from core.events.bid import Bid
from core.events.construction import ConstructionCheck, HouseBuilt
from core.events.eviction import Evicted
from core.events.income import AgentIncomeReceived
from core.events.landlord_migration import LandlordArrival, LandlordEntered, LandlordLeft
from core.events.maintenance import MaintenanceDue
from core.events.migration import AgentEntered, AgentLeft
from core.events.rent import RentCollected, RentDue, RentExpired, RentStarted
from core.events.tax import WealthTaxDeducted, WealthTaxDue

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
    | ConstructionCheck
    | HouseBuilt
    | MaintenanceDue
    | LandlordArrival
    | LandlordEntered
    | LandlordLeft
    | WealthTaxDeducted
    | WealthTaxDue
)

__all__ = [
    "AgentEntered",
    "AgentIncomeReceived",
    "AgentLeft",
    "ApplyResult",
    "AuctionClear",
    "Bid",
    "ConstructionCheck",
    "EventType",
    "Evicted",
    "HouseAged",
    "HouseBuilt",
    "HouseDemolished",
    "LandlordArrival",
    "LandlordEntered",
    "LandlordLeft",
    "MaintenanceDue",
    "RentCollected",
    "RentDue",
    "RentExpired",
    "RentStarted",
    "WealthTaxDeducted",
    "WealthTaxDue",
]
