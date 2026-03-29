from typing import TYPE_CHECKING

from pandera.typing import DataFrame

from analytics.gold.housed_renter_wealth.schema import HousedRenterWealth
from analytics.silver.occupancy.schema import OccupancyLog
from analytics.silver.wealth.schema import WealthLog

if TYPE_CHECKING:
    import pandas as pd


def build_housed_renter_wealth(
    wealth: DataFrame[WealthLog],
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[HousedRenterWealth]:
    """Keep only wealth rows where the renter is housed (exclude homeless and landlord)."""
    housed_agents: pd.DataFrame = (
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

    return (
        wealth
        .query(f"{WealthLog.agent} != 'landlord'")
        .merge(housed_agents, on=[WealthLog.time, WealthLog.agent])
        .reset_index(drop=True)
        .pipe(HousedRenterWealth.validate)
    )
