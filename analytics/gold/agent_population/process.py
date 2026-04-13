import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.smooth import lowess_smooth_xy
from core.entity.agent import Agent
from core.market import HousingMarket


def build_agent_population(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
    *,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.25,
    lowess_it: int = 0,
) -> DataFrame[AgentPopulation]:
    initial_count = len(initial_market.entities_of_type(Agent))
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    entries = (
        facts
        .query(f"{EventFact.event_type} == 'agent_entered'")
        .groupby(EventFact.time)
        .size()
    )
    exits = (
        facts
        .query(f"{EventFact.event_type} == 'agent_left'")
        .groupby(EventFact.time)
        .size()
    )

    combined = (
        pd.DataFrame({"entered": entries, "left": exits})
        .fillna(0)
        .astype(int)
    )
    if combined.empty:
        combined = pd.DataFrame({"entered": [0], "left": [0]}, index=[0.0])
        event_times = [0.0]
    else:
        # Populate the timeline at *all* event times (not just enter/leave) so smoothing is visible.
        # Missing times imply 0 entries/exits at that time.
        combined = combined.reindex(event_times, fill_value=0)

    combined[AgentPopulation.count] = initial_count + (combined["entered"] - combined["left"]).cumsum()
    combined[AgentPopulation.count_smooth] = combined[AgentPopulation.count].astype(float)
    if smooth_lowess:
        x = combined.index.to_numpy(dtype=float)
        y = combined[AgentPopulation.count_smooth].to_numpy(dtype=float)
        combined[AgentPopulation.count_smooth] = lowess_smooth_xy(x, y, frac=lowess_frac, it=lowess_it).clip(min=0.0)

    return (
        combined
        .rename_axis(AgentPopulation.time)
        .reset_index()
        [[AgentPopulation.time, AgentPopulation.count, AgentPopulation.count_smooth, "entered", "left"]]
        .pipe(AgentPopulation.validate)
    )
