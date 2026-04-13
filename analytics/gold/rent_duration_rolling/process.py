from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_rolling_stat_ci
from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.silver.rent_duration.schema import RentDuration


def build_rent_duration_rolling(
    rd: DataFrame[RentDuration],
    window: int = 10,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[RentDurationRolling]:
    sorted_df = rd.sort_values(RentDuration.time)
    rolling = sorted_df[RentDuration.duration].rolling(window, min_periods=1)
    stat, lo, hi = bootstrap_rolling_stat_ci(
        sorted_df[RentDuration.duration].to_numpy(),
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
        .pipe(RentDurationRolling.validate)
    )
