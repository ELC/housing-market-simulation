import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.occupancy.schema import OccupancyLog
from core.entity.agent import Agent
from core.entity.house import House
from core.market import HousingMarket


def project_occupancy(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[OccupancyLog]:
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    starts = facts.query(f"{EventFact.event_type} == 'rent_started'")[
        [EventFact.time, EventFact.house_id, EventFact.agent_id]
    ].rename(
        columns={
            EventFact.house_id: OccupancyLog.house,
            EventFact.agent_id: OccupancyLog.occupant,
        }
    )

    evicts = (
        facts
        .query(f"{EventFact.event_type} == 'evicted'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "vacant"})
    )

    houses = initial_market.entities_of_type(House)
    agents = initial_market.entities_of_type(Agent)
    house_names: dict[str, str] = {h.id: h.name for h in houses}
    agent_names: dict[str, str] = {a.id: a.name for a in agents}

    initials = pd.DataFrame({
        EventFact.time: [0.0] * len(houses),
        OccupancyLog.house: [h.id for h in houses],
        OccupancyLog.occupant: [h.occupant_id() or "vacant" for h in houses],
    })

    stacked = (
        pd
        .concat([initials, starts, evicts], ignore_index=True)
        .sort_values(EventFact.time)
        .groupby([OccupancyLog.house, EventFact.time])[OccupancyLog.occupant]
        .last()
        .unstack(OccupancyLog.house)
        .reindex(event_times)
        .ffill()
        .rename_axis(columns=OccupancyLog.house)
        .stack()
    )
    assert isinstance(stacked, pd.Series)

    return (
        stacked
        .rename(OccupancyLog.occupant)
        .reset_index()
        .assign(
            **{
                OccupancyLog.house: lambda df: df[OccupancyLog.house].map(house_names),
                OccupancyLog.occupant: lambda df: df[OccupancyLog.occupant].replace(agent_names),
            }
        )
        .pipe(OccupancyLog.validate)
    )
