import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.wealth.schema import WealthLog
from core.market import HousingMarket


def project_wealth(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[WealthLog]:
    """Cumulative agent wealth via a signed-delta ledger, ffilled to every event time."""
    house_owners: dict[str, str] = {h.id: h.owner_id for h in initial_market.houses}
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    inc: pd.DataFrame = (
        facts.query(f"{EventFact.event_type} == 'income'")
        [[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
    )

    rent: pd.DataFrame = facts.query(f"{EventFact.event_type} == 'rent_collected'")

    debits: pd.DataFrame = (
        rent[[EventFact.time, EventFact.agent_id, EventFact.amount]]
        .rename(columns={EventFact.agent_id: WealthLog.agent, EventFact.amount: "delta"})
        .assign(delta=lambda df: -df["delta"])
    )

    credits: pd.DataFrame = (
        rent[[EventFact.time, EventFact.house_id, EventFact.amount]]
        .assign(**{WealthLog.agent: lambda df: df[EventFact.house_id].map(house_owners)})
        .drop(columns=[EventFact.house_id])
        .rename(columns={EventFact.amount: "delta"})
    )

    initials: pd.DataFrame = pd.DataFrame(
        {
            EventFact.time: [0.0] * len(initial_market.agents),
            WealthLog.agent: [a.id for a in initial_market.agents],
            "delta": [a.money for a in initial_market.agents],
        }
    )

    return (
        pd.concat([initials, inc, debits, credits], ignore_index=True)
        .sort_values(EventFact.time, kind="mergesort")
        .assign(**{WealthLog.money: lambda df: df.groupby(WealthLog.agent)["delta"].cumsum()})
        .groupby([WealthLog.agent, EventFact.time])[WealthLog.money]
        .last()
        .unstack(WealthLog.agent)
        .reindex(event_times)
        .ffill()
        .rename_axis(columns=WealthLog.agent)
        .stack()
        .rename(WealthLog.money)
        .reset_index()
        .pipe(WealthLog.validate)
    )
