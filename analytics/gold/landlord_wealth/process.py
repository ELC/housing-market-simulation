import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.landlord_wealth.schema import GoldLandlordWealth


def build_landlord_wealth(aggregate: pd.DataFrame) -> DataFrame[GoldLandlordWealth]:
    if aggregate.empty:
        return GoldLandlordWealth.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="money",
    )
    return GoldLandlordWealth.validate(stats.drop(columns=["n"], errors="ignore"))
