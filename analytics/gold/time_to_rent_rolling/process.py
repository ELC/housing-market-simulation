from pandera.typing import DataFrame

from analytics.gold.time_to_rent_rolling.schema import TimeToRentRolling
from analytics.silver.time_to_rent.schema import TimeToRent


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
