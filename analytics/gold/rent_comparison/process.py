import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.rent_comparison.schema import RentComparison
from analytics.silver.asking_rent.schema import HouseRentLog
from analytics.silver.rent_payments.schema import RentLog


def build_rent_comparison(
    rent: DataFrame[RentLog],
    asking: DataFrame[HouseRentLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[RentComparison]:
    paid = rent[[RentLog.time, RentLog.amount]].assign(kind="paid")
    asked = asking[[HouseRentLog.time]].assign(amount=asking[HouseRentLog.rent].values, kind="asked")
    df = pd.concat([paid, asked], ignore_index=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[RentComparison.time, RentComparison.kind],
        value_col=RentComparison.amount,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(
            stats,
            x_col=RentComparison.time,
            group_cols=[RentComparison.kind],
            frac=lowess_frac,
            it=lowess_it,
            smooth_band=True,
        )
    return df.merge(stats, on=[RentComparison.time, RentComparison.kind], how="left").pipe(RentComparison.validate)
