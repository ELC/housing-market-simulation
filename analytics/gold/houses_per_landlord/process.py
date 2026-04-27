import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.houses_per_landlord.schema import GoldHousesPerLandlord


def build_houses_per_landlord(aggregate: pd.DataFrame) -> DataFrame[GoldHousesPerLandlord]:
    if aggregate.empty:
        return GoldHousesPerLandlord.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="ratio",
    )
    return GoldHousesPerLandlord.validate(stats.drop(columns=["n"], errors="ignore"))
