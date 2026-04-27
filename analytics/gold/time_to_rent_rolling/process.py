import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.silver.time_to_rent.schema import TimeToRent


def build_time_to_rent_rolling(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[TimeToRentRolling]:
    if aggregate.empty:
        return TimeToRentRolling.validate(pd.DataFrame(
            columns=[TimeToRentRolling.time, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[TimeToRent.time],
        value_col=TimeToRent.duration,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return TimeToRentRolling.validate(stats.drop(columns=["n"], errors="ignore"))
