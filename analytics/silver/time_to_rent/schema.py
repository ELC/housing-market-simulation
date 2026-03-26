import pandera as pa
from pandera.typing import Series


class TimeToRent(pa.DataFrameModel):
    """Silver: duration from vacancy to next rental for each house."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
