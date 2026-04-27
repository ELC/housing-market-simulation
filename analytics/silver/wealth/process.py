import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.wealth.schema import WealthLog
from core.events import (
    AgentEntered,
    AgentIncomeReceived,
    AgentLeft,
    HouseBuilt,
    LandlordEntered,
    LandlordLeft,
    RentCollected,
    WealthTaxDeducted,
)


def project_wealth(
    facts: DataFrame[EventFact],
    agent_names: dict[str, str] | None = None,
) -> DataFrame[WealthLog]:
    if agent_names is None:
        agent_names = {}

    house_owners: dict[str, str] = {}
    built = facts.query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")
    for _, row in built.iterrows():
        hid = row[EventFact.house_id]
        aid = row[EventFact.agent_id]
        if hid and aid:
            house_owners[hid] = aid

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

    # Agents enter via ``AgentEntered`` / ``LandlordEntered`` events that
    # carry the seed endowment as ``amount``. Treating these as t=entry
    # credits is what makes wealth fully derivable from the event log.
    entry_event_types = {AgentEntered.event_name(), LandlordEntered.event_name()}
    entries = (
        facts[facts[EventFact.event_type].isin(entry_event_types)][
            [EventFact.time, EventFact.agent_id, EventFact.amount]
        ]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
        .fillna({"delta": 0.0})
    )

    all_frames = [entries, inc, debits, credits, tax]
    combined = pd.concat(all_frames, ignore_index=True)

    if combined.empty:
        return WealthLog.validate(
            pd.DataFrame(columns=[WealthLog.time, WealthLog.agent, WealthLog.money])
        )

    pivot = (
        combined
        .sort_values(EventFact.time, kind="mergesort")
        .assign(**{WealthLog.money: lambda df: df.groupby(WealthLog.agent)["delta"].cumsum()})
        .groupby([WealthLog.agent, EventFact.time])[WealthLog.money]
        .last()
        .unstack(WealthLog.agent)
        .reindex(event_times)
        .ffill()
    )

    agent_left_types = {AgentLeft.event_name(), LandlordLeft.event_name()}
    departures = (
        facts[facts[EventFact.event_type].isin(agent_left_types)]
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
        .assign(**{WealthLog.agent: lambda df: df[WealthLog.agent].map(agent_names).fillna(df[WealthLog.agent])})
        .pipe(WealthLog.validate)
    )
