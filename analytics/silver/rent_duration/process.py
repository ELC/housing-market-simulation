import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.rent_duration.schema import RentDuration
from core.events import Evicted, RentExpired, RentStarted


def project_rent_duration(
    facts: DataFrame[EventFact],
    agent_names: dict[str, str] | None = None,
    house_names: dict[str, str] | None = None,
) -> DataFrame[RentDuration]:
    if agent_names is None:
        agent_names = {}
    if house_names is None:
        house_names = {}

    rent_starts = (
        facts
        .query(f"{EventFact.event_type} == '{RentStarted.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.time: "start"})
        .sort_values("start")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    rent_ends = (
        pd.concat(
            [
                facts.query(f"{EventFact.event_type} == '{Evicted.event_name()}'")[[EventFact.time, EventFact.house_id, EventFact.agent_id]],
                facts.query(f"{EventFact.event_type} == '{RentExpired.event_name()}'")[[EventFact.time, EventFact.house_id, EventFact.agent_id]],
            ],
            ignore_index=True,
        )
        .rename(columns={EventFact.time: "end"})
        .sort_values("end")
        .assign(rank=lambda df: df.groupby(EventFact.house_id).cumcount())
    )

    merged = rent_starts.merge(rent_ends, on=[EventFact.house_id, "rank"])
    if merged.empty:
        return RentDuration.validate(
            pd.DataFrame(columns=[RentDuration.time, RentDuration.house, RentDuration.tenant, RentDuration.duration])
        )

    return (
        merged
        .assign(**{
            RentDuration.time: lambda df: df["end"],
            RentDuration.house: lambda df: df[EventFact.house_id].map(house_names).fillna(df[EventFact.house_id]),
            RentDuration.tenant: lambda df: df[EventFact.agent_id].map(agent_names).fillna(df[EventFact.agent_id]),
            RentDuration.duration: lambda df: df["end"] - df["start"],
        })[[RentDuration.time, RentDuration.house, RentDuration.tenant, RentDuration.duration]]
        .pipe(RentDuration.validate)
    )
