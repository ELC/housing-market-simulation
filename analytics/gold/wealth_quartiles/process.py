import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.silver.wealth.schema import WealthLog


def build_wealth_quartiles(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[WealthQuartiles]:
    if aggregate.empty:
        return WealthQuartiles.validate(pd.DataFrame(
            columns=[WealthQuartiles.time, WealthQuartiles.quartile, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[WealthQuartiles.time, WealthQuartiles.quartile],
        value_col=WealthLog.money,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return WealthQuartiles.validate(stats.drop(columns=["n"], errors="ignore"))
