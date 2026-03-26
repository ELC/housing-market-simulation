import pandera as pa
from pandera.typing import Series


class HouseRents(pa.DataFrameModel):
    """Gold: per-house asking rent time series."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    rent: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
