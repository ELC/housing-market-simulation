from pandera.typing import DataFrame

from analytics.gold.bootstrap import bootstrap_mean_ci_by_group
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
) -> DataFrame[HousedRenterWealth]:
    housed_agents = (
        occupancy
        .query(f"{OccupancyLog.occupant} != 'vacant'")
        [["run_id", OccupancyLog.time, OccupancyLog.occupant]]
        .rename(
            columns={
                OccupancyLog.time: WealthLog.time,
                OccupancyLog.occupant: WealthLog.agent,
            },
        )
        .drop_duplicates()
    )

    mask = ~wealth[WealthLog.agent].isin(list(owner_names))
    df = (
        wealth[mask]
        .merge(housed_agents, on=["run_id", WealthLog.time, WealthLog.agent])
        .reset_index(drop=True)
    )
    stats = bootstrap_mean_ci_by_group(
        df,
        group_cols=[HousedRenterWealth.time],
        value_col=HousedRenterWealth.money,
        run_col="run_id",
        n_boot=n_boot,
        ci=ci,
        seed=seed,
        max_resample_size=max_resample_size,
    )
    return (
        df
        .merge(stats, on=[HousedRenterWealth.time], how="left")
        .drop(columns=["run_id"])
        .pipe(HousedRenterWealth.validate)
    )
