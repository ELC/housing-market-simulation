import numpy as np
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_rolling_stat_ci
from analytics.gold.smooth import lowess_smooth_xy
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
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
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
    if smooth_lowess:
        x = sorted_df[RentDuration.time].to_numpy()
        stat = lowess_smooth_xy(x, stat, frac=lowess_frac, it=lowess_it)
        lo = lowess_smooth_xy(x, lo, frac=lowess_frac, it=lowess_it)
        hi = lowess_smooth_xy(x, hi, frac=lowess_frac, it=lowess_it)
        lo2 = np.minimum(lo, hi)
        hi2 = np.maximum(lo, hi)
        lo = np.minimum(lo2, stat)
        hi = np.maximum(hi2, stat)
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
