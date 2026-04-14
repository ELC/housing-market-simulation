import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.wealth.schema import WealthLog
from core.entity.agent import Agent
from core.entity.house import House
from core.events import AgentIncomeReceived, AgentLeft, RentCollected, WealthTaxDeducted
from core.market import HousingMarket


def project_wealth(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
    agent_names: dict[str, str] | None = None,
) -> DataFrame[WealthLog]:
    agents = initial_market.entities_of_type(Agent)
    if agent_names is None:
        agent_names = {a.id: a.name for a in agents}
    house_owners: dict[str, str] = {h.id: h.owner_id for h in initial_market.entities_of_type(House)}
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    inc = facts.query(f"{EventFact.event_type} == '{AgentIncomeReceived.event_name()}'")[
        [EventFact.time, EventFact.agent_id, EventFact.amount]
    ].rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})

    rent = facts.query(f"{EventFact.event_type} == '{RentCollected.event_name()}'")

    debits = (
        rent[[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
        .assign(delta=lambda df: -df["delta"])
    )

    credits = (
        rent[[EventFact.time, EventFact.house_id, EventFact.amount]]
        .assign(**{WealthLog.agent: lambda df: df[EventFact.house_id].map(house_owners)})
        .drop(columns=[EventFact.house_id])
        .rename(columns={EventFact.amount: "delta"})
    )

    tax = (
        facts.query(f"{EventFact.event_type} == '{WealthTaxDeducted.event_name()}'")[
            [EventFact.time, EventFact.agent_id, EventFact.amount]
        ]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
        .assign(delta=lambda df: -df["delta"])
    )

    agents = initial_market.entities_of_type(Agent)
    initials = pd.DataFrame({
        EventFact.time: [0.0] * len(agents),
        WealthLog.agent: [a.id for a in agents],
        "delta": [a.money for a in agents],
    })

    pivot = (
        pd
        .concat([initials, inc, debits, credits, tax], ignore_index=True)
        .sort_values(EventFact.time, kind="mergesort")
        .assign(**{WealthLog.money: lambda df: df.groupby(WealthLog.agent)["delta"].cumsum()})
        .groupby([WealthLog.agent, EventFact.time])[WealthLog.money]
        .last()
        .unstack(WealthLog.agent)
        .reindex(event_times)
        .ffill()
    )

    departures = (
        facts.query(f"{EventFact.event_type} == '{AgentLeft.event_name()}'")
        .set_index(EventFact.agent_id)[EventFact.time]
    )
    for aid, dep_time in departures.items():
        if aid in pivot.columns:
            pivot.loc[pivot.index > dep_time, aid] = float("nan")

    stacked = (
        pivot
        .rename_axis(columns=WealthLog.agent)
        .stack()
        .dropna()
    )
    assert isinstance(stacked, pd.Series)

    return (
        stacked
        .rename(WealthLog.money)
        .reset_index()
        .assign(**{WealthLog.agent: lambda df: df[WealthLog.agent].map(agent_names)})
        .pipe(WealthLog.validate)
    )
