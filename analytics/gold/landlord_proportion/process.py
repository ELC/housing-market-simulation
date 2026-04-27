import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_logit_ci_by_group
from analytics.gold.landlord_proportion.schema import GoldLandlordProportion


def build_landlord_proportion(aggregate: pd.DataFrame) -> DataFrame[GoldLandlordProportion]:
    if aggregate.empty:
        return GoldLandlordProportion.validate(
            pd.DataFrame(columns=["time", "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_logit_ci_by_group(
        aggregate,
        group_cols=["time"],
        value_col="proportion",
    )
    return GoldLandlordProportion.validate(stats.drop(columns=["n"], errors="ignore"))
