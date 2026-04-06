import pandas as pd
from pandera.typing import DataFrame

from analytics.bronze.event_facts.schema import EventFact
from analytics.gold.agent_population.schema import AgentPopulation
from core.entity.agent import Agent
from core.market import HousingMarket


def build_agent_population(
    facts: DataFrame[EventFact],
    initial_market: HousingMarket,
) -> DataFrame[AgentPopulation]:
    initial_count = len(initial_market.entities_of_type(Agent))

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

    combined[AgentPopulation.count] = initial_count + (combined["entered"] - combined["left"]).cumsum()

    return (
        combined
        .rename_axis(AgentPopulation.time)
        .reset_index()
        [[AgentPopulation.time, AgentPopulation.count, "entered", "left"]]
        .pipe(AgentPopulation.validate)
    )
