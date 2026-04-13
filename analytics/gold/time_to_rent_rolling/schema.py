import pandera as pa
from pandera.typing import Series


class TimeToRentRolling(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)
    rolling_mean: Series[float]
    rolling_std: Series[float]
    rolling_ci_low: Series[float]
    rolling_ci_high: Series[float]

    class Config:
        coerce = True
