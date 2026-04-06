from pandera.typing import DataFrame

from analytics.gold.wealth_quartiles.schema import WealthQuartiles
from analytics.gold.wealth_spread.schema import WealthSpread


def build_wealth_spread(
    quartiles: DataFrame[WealthQuartiles],
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
    return (
        paired[[WealthQuartiles.time, WealthSpread.spread]]
        .reset_index(drop=True)
        .pipe(WealthSpread.validate)
    )
