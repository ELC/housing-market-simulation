import pandera as pa
from pandera.typing import Series


class HouseRentLog(pa.DataFrameModel):
    """Silver: reconstructed asking rent per house at each event time."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    rent: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
