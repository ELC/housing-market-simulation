from typing import TypedDict

import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from core.agent import Agent
from core.events import (
    AgentIncomeReceived,
    AuctionClear,
    Bid,
    Event,
    Evicted,
    RentCollected,
    RentStarted,
)
from core.market import HousingMarket


class EventRow(TypedDict):
    time: float
    event_type: str
    agent_id: str | None
    house_id: str | None
    amount: float | None


def event_to_row(event: Event, agents: dict[str, Agent]) -> EventRow:
    match event:
        case AgentIncomeReceived(time=t, agent_id=aid):
            a = agents[aid]
            return EventRow(
                time=t,
                event_type="income",
                agent_id=aid,
                house_id=None,
                amount=a.income * (1 - a.spend_rate),
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


def build_fact_table(
    event_log: list[Event],
    initial_market: HousingMarket,
) -> DataFrame[EventFact]:
    agents: dict[str, Agent] = {a.id: a for a in initial_market.agents}
    rows: list[EventRow] = [
        r for e in event_log if (r := event_to_row(e, agents)) is not None
    ]
    df = pd.DataFrame(rows)
    return EventFact.validate(df.reset_index(drop=True))
