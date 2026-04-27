import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.rent_to_income.schema import GoldRentToIncome


def build_rent_to_income(aggregate: pd.DataFrame) -> DataFrame[GoldRentToIncome]:
    if aggregate.empty:
        return GoldRentToIncome.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="ratio",
    )
    return GoldRentToIncome.validate(stats.drop(columns=["n"], errors="ignore"))
