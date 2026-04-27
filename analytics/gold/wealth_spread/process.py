import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.wealth_spread.schema import WealthSpread


def build_wealth_spread(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[WealthSpread]:
    if aggregate.empty:
        return WealthSpread.validate(pd.DataFrame(
            columns=[WealthSpread.time, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[WealthSpread.time],
        value_col="spread",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return WealthSpread.validate(stats.drop(columns=["n"], errors="ignore"))
