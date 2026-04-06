import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.occupancy.schema import OccupancyLog
from core.entity.agent import Agent
from core.entity.house import House
from core.entity.house.state import ConstructionState
from core.market import HousingMarket


def _initial_occupant(house: House) -> str:
    if isinstance(house.state, ConstructionState):
        return "construction"
    return house.occupant_id() or "vacant"


def project_occupancy(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
    agent_names: dict[str, str] | None = None,
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

    expired = (
        facts
        .query(f"{EventFact.event_type} == 'rent_expired'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "vacant"})
    )

    demolished = (
        facts
        .query(f"{EventFact.event_type} == 'house_demolished'")[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "demolished"})
    )

    rebuilt_raw = facts.query(f"{EventFact.event_type} == 'house_rebuilt'")
    rebuilt = (
        rebuilt_raw[[EventFact.time, EventFact.house_id]]
        .rename(columns={EventFact.house_id: OccupancyLog.house})
        .assign(**{OccupancyLog.occupant: "construction"})
    )

    rebuild_completions = pd.DataFrame([
        {
            EventFact.time: row[EventFact.time] + row[EventFact.amount],
            OccupancyLog.house: row[EventFact.house_id],
            OccupancyLog.occupant: "vacant",
        }
        for _, row in rebuilt_raw.iterrows()
    ])

    houses = initial_market.entities_of_type(House)
    house_names: dict[str, str] = {h.id: h.name for h in houses}
    if agent_names is None:
        agent_names = {a.id: a.name for a in initial_market.entities_of_type(Agent)}

    initials = pd.DataFrame({
        EventFact.time: [0.0] * len(houses),
        OccupancyLog.house: [h.id for h in houses],
        OccupancyLog.occupant: [_initial_occupant(h) for h in houses],
    })

    completions = pd.DataFrame([
        {
            EventFact.time: float(h.state.remaining_time),
            OccupancyLog.house: h.id,
            OccupancyLog.occupant: "vacant",
        }
        for h in houses
        if isinstance(h.state, ConstructionState)
    ])

    stacked = (
        pd
        .concat([initials, completions, starts, evicts, expired, demolished, rebuilt, rebuild_completions], ignore_index=True)
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
