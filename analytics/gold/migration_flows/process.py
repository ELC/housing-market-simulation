import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.migration_flows.schema import MigrationFlows


def build_migration_flows(
    population: DataFrame[AgentPopulation],
) -> DataFrame[MigrationFlows]:
    return (
        pd.melt(
            population,
            id_vars=[AgentPopulation.time],
            value_vars=["entered", "left"],
            var_name=MigrationFlows.direction,
            value_name=MigrationFlows.agents,
        )
        .pipe(MigrationFlows.validate)
    )
