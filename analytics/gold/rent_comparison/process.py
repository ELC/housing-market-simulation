import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.rent_comparison.schema import RentComparison
from analytics.silver.rent_payments.schema import RentLog


def build_rent_comparison(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[RentComparison]:
    if aggregate.empty:
        return RentComparison.validate(pd.DataFrame(
            columns=[RentComparison.time, RentComparison.kind, "mean", "ci_low", "ci_high"],
        ))
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[RentComparison.time, RentComparison.kind],
        value_col=RentLog.amount,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return RentComparison.validate(stats.drop(columns=["n"], errors="ignore"))
