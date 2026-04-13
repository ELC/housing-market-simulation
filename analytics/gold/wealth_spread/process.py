from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread


def build_wealth_spread(
    quartiles: DataFrame[WealthQuartiles],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[WealthSpread]:
    sort_cols = ["run_id", WealthQuartiles.time, WealthQuartiles.money]
    group_cols = ["run_id", WealthQuartiles.time]
    merge_on = ["run_id", WealthQuartiles.time, "rank"]

    q1 = (
        quartiles[quartiles[WealthQuartiles.quartile] == "Q1"]
        .sort_values(sort_cols)
        .assign(rank=lambda df: df.groupby(group_cols).cumcount())
    )
    q4 = (
        quartiles[quartiles[WealthQuartiles.quartile] == "Q4"]
        .sort_values(sort_cols)
        .assign(rank=lambda df: df.groupby(group_cols).cumcount())
    )
    paired = q1.merge(q4, on=merge_on, suffixes=("_q1", "_q4"))
    paired[WealthSpread.spread] = (
        paired[f"{WealthQuartiles.money}_q4"] - paired[f"{WealthQuartiles.money}_q1"]
    )
    df = paired[["run_id", WealthQuartiles.time, WealthSpread.spread]].reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[WealthSpread.time],
        value_col=WealthSpread.spread,
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df
        .merge(stats, on=[WealthSpread.time], how="left")
        .drop(columns=["run_id"])
        .pipe(WealthSpread.validate)
    )
