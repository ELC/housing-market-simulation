import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.silver.population.schema import PopulationLog


def build_agent_population(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[AgentPopulation]:
    if aggregate.empty:
        return AgentPopulation.validate(pd.DataFrame(
            columns=[PopulationLog.time, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[PopulationLog.time],
        value_col="count",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return AgentPopulation.validate(stats.drop(columns=["n"], errors="ignore"))
