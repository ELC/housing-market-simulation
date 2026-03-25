import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.schemas import (
    HouseRents,
    HousedRenterWealth,
    OccupancyTimeline,
    RentComparison,
    RentPayments,
    RenterWealth,
    TimeToRentRolling,
    VacancyCount,
)
from analytics.silver.schemas import (
    HouseRentLog,
    OccupancyLog,
    RentLog,
    TimeToRent,
    WealthLog,
)


def build_renter_wealth(
    wealth: DataFrame[WealthLog],
) -> DataFrame[RenterWealth]:
    """Filter wealth to renters only (exclude landlord)."""
    return (
        wealth.query(f"{WealthLog.agent} != 'landlord'")
        .reset_index(drop=True)
        .pipe(RenterWealth.validate)
    )


def build_housed_renter_wealth(
    wealth: DataFrame[WealthLog],
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[HousedRenterWealth]:
    """Keep only wealth rows where the renter is housed (exclude homeless and landlord)."""
    housed_agents: pd.DataFrame = (
        occupancy.query(f"{OccupancyLog.occupant} != 'vacant'")[
            [OccupancyLog.time, OccupancyLog.occupant]
        ]
        .rename(
            columns={
                OccupancyLog.time: WealthLog.time,
                OccupancyLog.occupant: WealthLog.agent,
            }
        )
        .drop_duplicates()
    )

    return (
        wealth.query(f"{WealthLog.agent} != 'landlord'")
        .merge(housed_agents, on=[WealthLog.time, WealthLog.agent])
        .reset_index(drop=True)
        .pipe(HousedRenterWealth.validate)
    )


def build_rent_payments(
    rent: DataFrame[RentLog],
) -> DataFrame[RentPayments]:
    """Pass-through: silver rent payments validated as gold."""
    return RentPayments.validate(rent.reset_index(drop=True))


def build_rent_comparison(
    rent: DataFrame[RentLog],
    asking: DataFrame[HouseRentLog],
) -> DataFrame[RentComparison]:
    """Combine paid and asked rent into a single long-form table."""
    paid: pd.DataFrame = (
        rent[[RentLog.time, RentLog.amount]]
        .assign(kind="paid")
    )
    asked: pd.DataFrame = (
        asking[[HouseRentLog.time]]
        .assign(amount=asking[HouseRentLog.rent].values, kind="asked")
    )
    return (
        pd.concat([paid, asked], ignore_index=True)
        .pipe(RentComparison.validate)
    )


def build_house_rents(
    asking: DataFrame[HouseRentLog],
) -> DataFrame[HouseRents]:
    """Pass-through: silver asking rents validated as gold."""
    return HouseRents.validate(asking.reset_index(drop=True))


def build_occupancy_timeline(
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[OccupancyTimeline]:
    """Pass-through: silver occupancy validated as gold."""
    return OccupancyTimeline.validate(occupancy.reset_index(drop=True))


def build_vacancy_count(
    occupancy: DataFrame[OccupancyLog],
) -> DataFrame[VacancyCount]:
    """Count vacant houses at each event time."""
    return (
        occupancy.assign(
            is_vacant=lambda df: (df[OccupancyLog.occupant] == "vacant").astype(int)
        )
        .groupby(OccupancyLog.time)["is_vacant"]
        .sum()
        .rename(VacancyCount.n_vacant)
        .reset_index()
        .pipe(VacancyCount.validate)
    )


def build_time_to_rent_rolling(
    ttr: DataFrame[TimeToRent],
    window: int = 10,
) -> DataFrame[TimeToRentRolling]:
    """Enrich time-to-rent with rolling mean and standard deviation."""
    sorted_df = ttr.sort_values(TimeToRent.time)
    rolling = sorted_df[TimeToRent.duration].rolling(window, min_periods=1)
    return (
        sorted_df.assign(
            rolling_mean=rolling.mean(),
            rolling_std=rolling.std().fillna(0),
        )
        .reset_index(drop=True)
        .pipe(TimeToRentRolling.validate)
    )
