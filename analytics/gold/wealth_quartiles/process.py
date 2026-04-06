import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.renter_wealth.schema import RenterWealth
from analytics.gold.wealth_quartiles.schema import WealthQuartiles


def build_wealth_quartiles(
    wealth: DataFrame[RenterWealth],
) -> DataFrame[WealthQuartiles]:
    df = wealth.copy()
    df[WealthQuartiles.quartile] = (
        df
        .groupby(RenterWealth.time)[RenterWealth.money]
        .transform(
            lambda x: pd.cut(
                x.rank(method="first", pct=True),
                bins=[0, 0.25, 0.5, 0.75, 1.0],
                labels=["Q1", "Q2", "Q3", "Q4"],
            )
        )
    )
    return df.reset_index(drop=True).pipe(WealthQuartiles.validate)
