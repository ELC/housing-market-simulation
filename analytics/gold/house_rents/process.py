from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.house_rents.schema import HouseRents
from analytics.silver.asking_rent.schema import HouseRentLog


def build_house_rents(
    asking: DataFrame[HouseRentLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[HouseRents]:
    df = asking.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[HouseRents.time],
        value_col=HouseRents.rent,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(stats, x_col=HouseRents.time, frac=lowess_frac, it=lowess_it, smooth_band=True)
    return df.merge(stats, on=[HouseRents.time], how="left").pipe(HouseRents.validate)
