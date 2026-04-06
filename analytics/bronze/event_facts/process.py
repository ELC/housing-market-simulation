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
    EventType,
    Evicted,
    HouseDemolished,
    HouseRebuilt,
    RentCollected,
    RentDue,
    RentExpired,
    RentStarted,
)
from core.market import HousingMarket


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
                event_type="income",
                agent_id=aid,
                house_id=None,
                amount=amt,
            )
        case RentStarted(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type="rent_started",
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case RentCollected(time=t, house_id=hid, tenant_id=tid, amount=amt):
            return EventRow(
                time=t,
                event_type="rent_collected",
                agent_id=tid,
                house_id=hid,
                amount=amt,
            )
        case Evicted(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type="evicted",
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case Bid(time=t, agent_id=aid, house_id=hid, price=p):
            return EventRow(
                time=t,
                event_type="bid",
                agent_id=aid,
                house_id=hid,
                amount=p,
            )
        case AuctionClear(time=t):
            return EventRow(
                time=t,
                event_type="auction_clear",
                agent_id=None,
                house_id=None,
                amount=None,
            )
        case RentExpired(time=t, house_id=hid, tenant_id=tid):
            return EventRow(
                time=t,
                event_type="rent_expired",
                agent_id=tid,
                house_id=hid,
                amount=None,
            )
        case HouseDemolished(time=t, house_id=hid):
            return EventRow(
                time=t,
                event_type="house_demolished",
                agent_id=None,
                house_id=hid,
                amount=None,
            )
        case HouseRebuilt(time=t, house_id=hid, construction_time=ct):
            return EventRow(
                time=t,
                event_type="house_rebuilt",
                agent_id=None,
                house_id=hid,
                amount=float(ct),
            )
        case AgentEntered(time=t, agent_id=aid):
            return EventRow(
                time=t,
                event_type="agent_entered",
                agent_id=aid,
                house_id=None,
                amount=None,
            )
        case AgentLeft(time=t, agent_id=aid):
            return EventRow(
                time=t,
                event_type="agent_left",
                agent_id=aid,
                house_id=None,
                amount=None,
            )
        case RentDue() | _:
            return None


def build_fact_table(
    event_log: Sequence[EventType],
    initial_market: HousingMarket,
) -> DataFrame[EventFact]:
    rows = [r for e in event_log if (r := event_to_row(e))]
    df = pd.DataFrame(rows)
    return EventFact.validate(df.reset_index(drop=True))
