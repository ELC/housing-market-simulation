import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.demolition_age.schema import GoldDemolitionAge


def build_demolition_age(aggregate: pd.DataFrame) -> DataFrame[GoldDemolitionAge]:
    if aggregate.empty:
        return GoldDemolitionAge.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="age",
    )
    return GoldDemolitionAge.validate(stats.drop(columns=["n"], errors="ignore"))
