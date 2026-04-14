import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.silver.population.schema import PopulationLog
from core.entity import Agent
from core.events import AgentEntered, AgentLeft
from core.market import HousingMarket


def project_population(
    facts: DataFrame[EventFact],
    market: HousingMarket,
) -> DataFrame[PopulationLog]:
    initial_agent_count = len(market.entities_of_type(Agent))
    event_times: list[float] = sorted(facts[EventFact.time].unique())

    entries = (
        facts
        .query(f"{EventFact.event_type} == '{AgentEntered.event_name()}'")
        .groupby(EventFact.time)
        .size()
    )
    exits = (
        facts
        .query(f"{EventFact.event_type} == '{AgentLeft.event_name()}'")
        .groupby(EventFact.time)
        .size()
    )

    combined = pd.DataFrame({"entered": entries, "left": exits}).fillna(0).astype(int)
    if combined.empty:
        combined = pd.DataFrame({"entered": [0], "left": [0]}, index=[0.0])
    else:
        combined = combined.reindex(event_times, fill_value=0)

    combined["count"] = initial_agent_count + (
        combined["entered"] - combined["left"]
    ).cumsum()

    return (
        combined
        .rename_axis("time")
        .reset_index()
        [["time", "count", "entered", "left"]]
        .pipe(PopulationLog.validate)
    )
