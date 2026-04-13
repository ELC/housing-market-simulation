from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_rolling_stat_ci
from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.silver.time_to_rent.schema import TimeToRent


def build_time_to_rent_rolling(
    ttr: DataFrame[TimeToRent],
    window: int = 10,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[TimeToRentRolling]:
    sorted_df = ttr.sort_values(TimeToRent.time)
    rolling = sorted_df[TimeToRent.duration].rolling(window, min_periods=1)
    stat, lo, hi = bootstrap_rolling_stat_ci(
        sorted_df[TimeToRent.duration].to_numpy(),
        window=window,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        sorted_df
        .assign(
            rolling_mean=stat,
            rolling_std=rolling.std().fillna(0),
            rolling_ci_low=lo,
            rolling_ci_high=hi,
        )
        .reset_index(drop=True)
        .pipe(TimeToRentRolling.validate)
    )
