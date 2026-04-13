import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.gold.wealth_quartiles.schema import WealthQuartiles


def build_wealth_quartiles(
    wealth: DataFrame[RenterWealth],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[WealthQuartiles]:
    df = wealth.copy()
    pct_rank = df.groupby(["run_id", RenterWealth.time], sort=False)[RenterWealth.money].rank(method="first", pct=True)
    df[WealthQuartiles.quartile] = pd.cut(
        pct_rank,
        bins=[0, 0.25, 0.5, 0.75, 1.0],
        labels=["Q1", "Q2", "Q3", "Q4"],
        include_lowest=True,
    )
    df = df.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[WealthQuartiles.time, WealthQuartiles.quartile],
        value_col=WealthQuartiles.money,
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df
        .merge(stats, on=[WealthQuartiles.time, WealthQuartiles.quartile], how="left")
        .pipe(WealthQuartiles.validate)
    )
