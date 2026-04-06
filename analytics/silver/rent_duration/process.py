import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.rent_duration.schema import RentDuration
from core.entity.agent import Agent
from core.entity.house import House
from core.market import HousingMarket


def project_rent_duration(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
    agent_names: dict[str, str] | None = None,
) -> DataFrame[RentDuration]:
    houses = initial_market.entities_of_type(House)
    house_names: dict[str, str] = {h.id: h.name for h in houses}
    if agent_names is None:
        agent_names = {a.id: a.name for a in initial_market.entities_of_type(Agent)}

    rent_starts = (
        facts
        .query(f"{EventFact.event_type} == 'rent_started'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "start"})
        .sort_values("start")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    rent_ends = (
        pd.concat(
            [
                facts.query(f"{EventFact.event_type} == 'evicted'")[[EventFact.time, EventFact.house_id, EventFact.agent_id]],
                facts.query(f"{EventFact.event_type} == 'rent_expired'")[[EventFact.time, EventFact.house_id, EventFact.agent_id]],
            ],
            ignore_index=True,
        )
        .rename(columns={EventFact.time: "end"})
        .sort_values("end")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    return (
        rent_starts
        .merge(rent_ends, on=[EventFact.house_id, "rank"])
        .assign(**{
            RentDuration.time: lambda df: df["end"],
            RentDuration.house: lambda df: df[EventFact.house_id].map(house_names),
            RentDuration.tenant: lambda df: df[EventFact.agent_id].map(agent_names),
            RentDuration.duration: lambda df: df["end"] - df["start"],
        })[[RentDuration.time, RentDuration.house, RentDuration.tenant, RentDuration.duration]]
        .pipe(RentDuration.validate)
    )
