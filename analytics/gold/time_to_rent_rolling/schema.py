import pandera as pa
from pandera.typing import Series


class TimeToRentRolling(pa.DataFrameModel):
    """Gold: time-to-rent enriched with rolling statistics."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)
    rolling_mean: Series[float]
    rolling_std: Series[float]

    class Config:
        coerce = True
