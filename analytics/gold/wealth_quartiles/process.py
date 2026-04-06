import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.gold.wealth_quartiles.schema import WealthQuartiles


def build_wealth_quartiles(
    wealth: DataFrame[RenterWealth],
) -> DataFrame[WealthQuartiles]:
    def _quartile_means(group: pd.DataFrame) -> pd.DataFrame:
        ranked = group[RenterWealth.money].rank(method="first", pct=True)
        labels = pd.cut(ranked, bins=[0, 0.25, 0.5, 0.75, 1.0], labels=["Q1", "Q2", "Q3", "Q4"])
        return group.assign(quartile=labels).groupby("quartile", observed=True)[RenterWealth.money].mean().reset_index()

    rows = (
        wealth
        .groupby(RenterWealth.time, group_keys=True)
        .apply(_quartile_means, include_groups=False)
        .reset_index(level=0)
        .rename(columns={RenterWealth.money: WealthQuartiles.mean_wealth})
    )[[WealthQuartiles.time, WealthQuartiles.quartile, WealthQuartiles.mean_wealth]]

    return rows.reset_index(drop=True).pipe(WealthQuartiles.validate)
