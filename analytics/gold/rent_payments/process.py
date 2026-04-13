from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.rent_payments.schema import RentPayments
from analytics.silver.rent_payments.schema import RentLog


def build_rent_payments(
    rent: DataFrame[RentLog],
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[RentPayments]:
    df = rent.reset_index(drop=True)
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[RentLog.time],
        value_col=RentLog.amount,
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return df.merge(stats, on=[RentLog.time], how="left").drop(columns=["run_id"], errors="ignore").pipe(RentPayments.validate)
