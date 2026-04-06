from pandera.typing import DataFrame

from analytics.gold.rent_duration_rolling.schema import RentDurationRolling
from analytics.silver.rent_duration.schema import RentDuration


def build_rent_duration_rolling(
    rd: DataFrame[RentDuration],
    window: int = 10,
) -> DataFrame[RentDurationRolling]:
    sorted_df = rd.sort_values(RentDuration.time)
    rolling = sorted_df[RentDuration.duration].rolling(window, min_periods=1)
    return (
        sorted_df
        .assign(
            rolling_mean=rolling.mean(),
            rolling_std=rolling.std().fillna(0),
        )
        .reset_index(drop=True)
        .pipe(RentDurationRolling.validate)
    )
