import pandas as pd
from pandera.typing import DataFrame

from analytics.gold.rent_comparison.schema import RentComparison
from analytics.silver.asking_rent.schema import HouseRentLog
from analytics.silver.rent_payments.schema import RentLog


def build_rent_comparison(
    rent: DataFrame[RentLog],
    asking: DataFrame[HouseRentLog],
) -> DataFrame[RentComparison]:
    """Combine paid and asked rent into a single long-form table."""
    paid = rent[[RentLog.time, RentLog.amount]].assign(kind="paid")
    asked = asking[[HouseRentLog.time]].assign(amount=asking[HouseRentLog.rent].values, kind="asked")
    return pd.concat([paid, asked], ignore_index=True).pipe(RentComparison.validate)
