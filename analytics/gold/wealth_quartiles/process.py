import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.gold.wealth_quartiles.schema import WealthQuartiles


def build_wealth_quartiles(
    wealth: DataFrame[RenterWealth],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[WealthQuartiles]:
    df = wealth.copy()
    pct_rank = df.groupby(RenterWealth.time, sort=False)[RenterWealth.money].rank(method="first", pct=True)
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
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(
            stats,
            x_col=WealthQuartiles.time,
            group_cols=[WealthQuartiles.quartile],
            frac=lowess_frac,
            it=lowess_it,
            smooth_band=True,
        )
    return df.merge(stats, on=[WealthQuartiles.time, WealthQuartiles.quartile], how="left").pipe(WealthQuartiles.validate)
