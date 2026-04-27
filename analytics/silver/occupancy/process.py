import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.occupancy.schema import OccupancyLog
from core.events import Evicted, HouseDemolished, RentExpired, RentStarted
from core.events.construction import HouseBuilt


def project_occupancy(
    facts: DataFrame[EventFact],
    agent_names: dict[str, str] | None = None,
    house_names: dict[str, str] | None = None,
) -> DataFrame[OccupancyLog]:
    if agent_names is None:
        agent_names = {}
    if house_names is None:
        house_names = {}

    event_times: list[float] = sorted(facts[EventFact.time].unique())

    starts = facts.query(f"{EventFact.event_type} == '{RentStarted.event_name()}'")[
        [EventFact.time, EventFact.house_id, EventFact.agent_id]
    ].rename(columns={
        EventFact.house_id: OccupancyLog.house,
        EventFact.agent_id: OccupancyLog.occupant,
    })

    evicts = (
        facts
        .query(f"{EventFact.event_type} == '{Evicted.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "vacant"})
    )

    expired = (
        facts
        .query(f"{EventFact.event_type} == '{RentExpired.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "vacant"})
    )

    demolished = (
        facts
        .query(f"{EventFact.event_type} == '{HouseDemolished.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "demolished"})
    )

    built = (
        facts
        .query(f"{EventFact.event_type} == '{HouseBuilt.event_name()}'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "construction"})
    )

    combined = pd.concat([starts, evicts, expired, demolished, built], ignore_index=True)
    if combined.empty:
        return OccupancyLog.validate(
            pd.DataFrame(columns=[OccupancyLog.time, OccupancyLog.house, OccupancyLog.occupant])
        )

    wide = (
        combined
        .sort_values(EventFact.time)
        .groupby([OccupancyLog.house, EventFact.time])[OccupancyLog.occupant]
        .last()
        .unstack(OccupancyLog.house)
        .reindex(event_times)
        .ffill()
        .rename_axis(columns=OccupancyLog.house)
    )
    stacked = wide.stack(future_stack=True).dropna()
    assert isinstance(stacked, pd.Series)

    return (
        stacked
        .rename(OccupancyLog.occupant)
        .reset_index()
        .assign(**{
            OccupancyLog.house: lambda df: df[OccupancyLog.house].map(house_names).fillna(df[OccupancyLog.house]),
            OccupancyLog.occupant: lambda df: df[OccupancyLog.occupant].replace(agent_names),
        })
        .pipe(OccupancyLog.validate)
    )
