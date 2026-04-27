import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.tenure.schema import GoldTenure


def build_tenure(aggregate: pd.DataFrame) -> DataFrame[GoldTenure]:
    if aggregate.empty:
        return GoldTenure.validate(
            pd.DataFrame(columns=["time", "kind", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time", "kind"],
        value_col="duration",
    )
    return GoldTenure.validate(stats.drop(columns=["n"], errors="ignore"))
