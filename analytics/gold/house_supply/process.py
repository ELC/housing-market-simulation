import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.house_supply.schema import GoldHouseSupply


def build_house_supply(aggregate: pd.DataFrame) -> DataFrame[GoldHouseSupply]:
    if aggregate.empty:
        return GoldHouseSupply.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="count",
    )
    return GoldHouseSupply.validate(stats.drop(columns=["n"], errors="ignore"))
