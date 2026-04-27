import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.landlord_population.schema import GoldLandlordPopulation


def build_landlord_population(aggregate: pd.DataFrame) -> DataFrame[GoldLandlordPopulation]:
    if aggregate.empty:
        return GoldLandlordPopulation.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="count",
    )
    return GoldLandlordPopulation.validate(stats.drop(columns=["n"], errors="ignore"))
