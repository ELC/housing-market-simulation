from pandera.typing import DataFrame

from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.silver.occupancy.schema import OccupancyLog
from analytics.silver.wealth.schema import WealthLog


def build_housed_renter_wealth(
    wealth: DataFrame[WealthLog],
    occupancy: DataFrame[OccupancyLog],
    owner_names: frozenset[str] = frozenset(),
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
    return (
        wealth[mask]
        .merge(housed_agents, on=[WealthLog.time, WealthLog.agent])
        .reset_index(drop=True)
        .pipe(HousedRenterWealth.validate)
    )
