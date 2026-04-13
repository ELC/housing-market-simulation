# pyright: reportUnknownMemberType=false
from pandera.typing import DataFrame

from analytics.gold.agent_population.schema import AgentPopulation
from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.silver.population.schema import PopulationLog


def build_agent_population(
    population: DataFrame[PopulationLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[AgentPopulation]:
    df = population.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[PopulationLog.time],
        value_col="count",
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df.merge(stats, on=[PopulationLog.time], how="left")
        .drop(columns=["run_id", "n"], errors="ignore")
        .pipe(AgentPopulation.validate)
    )
