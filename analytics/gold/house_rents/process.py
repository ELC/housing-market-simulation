from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.house_rents.schema import HouseRents
from analytics.silver.asking_rent.schema import HouseRentLog


def build_house_rents(
    asking: DataFrame[HouseRentLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[HouseRents]:
    df = asking.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[HouseRents.time],
        value_col=HouseRents.rent,
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return df.merge(stats, on=[HouseRents.time], how="left").drop(columns=["run_id"], errors="ignore").pipe(HouseRents.validate)
