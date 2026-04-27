import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.silver.wealth.schema import WealthLog


def build_housed_renter_wealth(
    aggregate: pd.DataFrame,
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
) -> DataFrame[HousedRenterWealth]:
    """Bootstrap CI for mean renter wealth (conditional on being housed).

    ``aggregate`` is the pre-aggregated ``(run_id, time, money)`` frame
    produced by
    :func:`analytics.silver.aggregates.housed_renter_wealth_aggregate`.
    """
    if aggregate.empty:
        return HousedRenterWealth.validate(
            pd.DataFrame(columns=[HousedRenterWealth.time, "mean", "ci_low", "ci_high"]),
        )
    stats = bootstrap_mean_ci_by_group(
        aggregate,
        group_cols=[WealthLog.time],
        value_col=WealthLog.money,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return HousedRenterWealth.validate(stats.drop(columns=["n"], errors="ignore"))
