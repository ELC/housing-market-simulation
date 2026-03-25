import pandera as pa
from pandera.typing import Series


class WealthLog(pa.DataFrameModel):
    """Silver: cumulative agent wealth at each event time."""

    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True


class OccupancyLog(pa.DataFrameModel):
    """Silver: house occupancy state at each event time."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    occupant: Series[str]

    class Config:
        coerce = True


class RentLog(pa.DataFrameModel):
    """Silver: individual rent payments."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class TimeToRent(pa.DataFrameModel):
    """Silver: duration from vacancy to next rental for each house."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class HouseRentLog(pa.DataFrameModel):
    """Silver: reconstructed asking rent per house at each event time."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    rent: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
