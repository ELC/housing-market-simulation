from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
from analytics.gold.smooth import lowess_smooth_stats
from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.silver.occupancy.schema import OccupancyLog
from analytics.silver.wealth.schema import WealthLog


def build_housed_renter_wealth(
    wealth: DataFrame[WealthLog],
    occupancy: DataFrame[OccupancyLog],
    owner_names: frozenset[str] = frozenset(),
    *,
    n_boot: int = 500,
    ci: float = 95.0,
    seed: int = 0,
    max_resample_size: int = 500,
    smooth_lowess: bool = True,
    lowess_frac: float = 0.15,
    lowess_it: int = 0,
) -> DataFrame[HousedRenterWealth]:
    housed_agents = (
        occupancy
        .query(f"{OccupancyLog.occupant} != 'vacant'")[[OccupancyLog.time, OccupancyLog.occupant]]
        .rename(
            columns={
                OccupancyLog.time: WealthLog.time,
                OccupancyLog.occupant: WealthLog.agent,
            }
        )
        .drop_duplicates()
    )

    mask = ~wealth[WealthLog.agent].isin(list(owner_names))
    df = (
        wealth[mask]
        .merge(housed_agents, on=[WealthLog.time, WealthLog.agent])
        .reset_index(drop=True)
    )
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[HousedRenterWealth.time],
        value_col=HousedRenterWealth.money,
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    if smooth_lowess:
        stats = lowess_smooth_stats(stats, x_col=HousedRenterWealth.time, frac=lowess_frac, it=lowess_it, smooth_band=True)
    return df.merge(stats, on=[HousedRenterWealth.time], how="left").pipe(HousedRenterWealth.validate)
