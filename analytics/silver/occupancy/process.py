import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.occupancy.schema import OccupancyLog
from core.market import HousingMarket


def project_occupancy(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[OccupancyLog]:
    """House occupancy from rent_started / evicted events, ffilled to every event time."""
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

    initials = pd.DataFrame({
        EventFact.time: [0.0] * len(initial_market.houses),
        OccupancyLog.house: [h.id for h in initial_market.houses],
        OccupancyLog.occupant: [h.occupant_id() or "vacant" for h in initial_market.houses],
    })

    return (
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
        .rename(OccupancyLog.occupant)
        .reset_index()
        .pipe(OccupancyLog.validate)
    )
