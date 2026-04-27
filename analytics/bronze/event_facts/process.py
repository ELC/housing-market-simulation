from collections.abc import Sequence
from typing import TypedDict

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from core.events import (
    AgentEntered,
    AgentIncomeReceived,
    AgentLeft,
    AuctionClear,
    Bid,
    ConstructionCheck,
    EventType,
    Evicted,
    HouseAged,
    HouseBuilt,
    HouseDemolished,
    LandlordArrival,
    LandlordEntered,
    LandlordLeft,
    MaintenanceDue,
    RentCollected,
    RentDue,
    RentExpired,
    RentStarted,
    WealthTaxDeducted,
    WealthTaxDue,
)
class EventRow(TypedDict):
    time: float
    event_type: str
    agent_id: str | None
    house_id: str | None
    amount: float | None


def event_to_row(event: EventType) -> EventRow | None:  # noqa: PLR0911
    match event:
        case AgentIncomeReceived(time=t, agent_id=aid, amount=amt):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=amt,
            )
        case RentStarted(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case RentCollected(time=t, house_id=hid, tenant_id=tid, amount=amt):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=tid,
                house_id=hid,
                amount=amt,
            )
        case Evicted(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case Bid(time=t, agent_id=aid, house_id=hid, price=p):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=hid,
                amount=p,
            )
        case AuctionClear(time=t):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=None,
                house_id=None,
                amount=None,
            )
        case RentExpired(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case HouseDemolished(time=t, house_id=hid, age=a):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=None,
                house_id=hid,
                amount=float(a),
            )
        case HouseAged(time=t, house_id=hid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=None,
                house_id=hid,
                amount=None,
            )
        case ConstructionCheck(time=t, landlord_id=lid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=lid,
                house_id=None,
                amount=None,
            )
        case HouseBuilt(time=t, landlord_id=lid, house_id=hid, cost=c):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=lid,
                house_id=hid,
                amount=c,
            )
        case MaintenanceDue(time=t, house_id=hid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=None,
                house_id=hid,
                amount=None,
            )
        case LandlordEntered(time=t, agent_id=aid, initial_money=m):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=m,
            )
        case LandlordLeft(time=t, agent_id=aid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=None,
            )
        case AgentEntered(time=t, agent_id=aid, initial_money=m):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=m,
            )
        case AgentLeft(time=t, agent_id=aid):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=None,
            )
        case WealthTaxDeducted(time=t, agent_id=aid, amount=amt):
            return EventRow(
                time=t,
                event_type=event.event_name(),
                agent_id=aid,
                house_id=None,
                amount=amt,
            )
        case RentDue() | LandlordArrival() | WealthTaxDue():
            return None


def build_fact_table(event_log: Sequence[EventType]) -> DataFrame[EventFact]:
    rows = [r for e in event_log if (r := event_to_row(e))]
    df = pd.DataFrame(rows)
    return EventFact.validate(df.reset_index(drop=True))
