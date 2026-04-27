import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.silver.rent_duration.schema import RentDuration


def build_rent_duration_rolling(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[RentDurationRolling]:
    if aggregate.empty:
        return RentDurationRolling.validate(pd.DataFrame(
            columns=[RentDurationRolling.time, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[RentDuration.time],
        value_col=RentDuration.duration,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return RentDurationRolling.validate(stats.drop(columns=["n"], errors="ignore"))
