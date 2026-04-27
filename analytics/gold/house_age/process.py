import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.house_age.schema import GoldHouseAge


def build_house_age(aggregate: pd.DataFrame) -> DataFrame[GoldHouseAge]:
    if aggregate.empty:
        return GoldHouseAge.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="age",
    )
    return GoldHouseAge.validate(stats.drop(columns=["n"], errors="ignore"))
