from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread


def build_wealth_spread(
    quartiles: DataFrame[WealthQuartiles],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[WealthSpread]:
    q1 = (
        quartiles[quartiles[WealthQuartiles.quartile] == "Q1"]
        .sort_values([WealthQuartiles.time, WealthQuartiles.money])
        .assign(rank=lambda df: df.groupby(WealthQuartiles.time).cumcount())
    )
    q4 = (
        quartiles[quartiles[WealthQuartiles.quartile] == "Q4"]
        .sort_values([WealthQuartiles.time, WealthQuartiles.money])
        .assign(rank=lambda df: df.groupby(WealthQuartiles.time).cumcount())
    )
    paired = q1.merge(q4, on=[WealthQuartiles.time, "rank"], suffixes=("_q1", "_q4"))
    paired[WealthSpread.spread] = (
        paired[f"{WealthQuartiles.money}_q4"] - paired[f"{WealthQuartiles.money}_q1"]
    )
    df = (
        paired[[WealthQuartiles.time, WealthSpread.spread]]
        .reset_index(drop=True)
    )
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[WealthSpread.time],
        value_col=WealthSpread.spread,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(stats, x_col=WealthSpread.time, frac=lowess_frac, it=lowess_it, smooth_band=True)
    return df.merge(stats, on=[WealthSpread.time], how="left").pipe(WealthSpread.validate)
