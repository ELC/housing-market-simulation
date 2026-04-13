from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.rent_payments.schema import RentPayments
from analytics.silver.rent_payments.schema import RentLog


def build_rent_payments(
    rent: DataFrame[RentLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[RentPayments]:
    df = rent.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[RentLog.time],
        value_col=RentLog.amount,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(stats, x_col=RentLog.time, frac=lowess_frac, it=lowess_it, smooth_band=True)
    return df.merge(stats, on=[RentLog.time], how="left").pipe(RentPayments.validate)
