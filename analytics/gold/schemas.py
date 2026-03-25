import pandera as pa
from pandera.typing import Series


class RenterWealth(pa.DataFrameModel):
    """Gold: per-renter wealth over time (landlord excluded)."""

    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True


class HousedRenterWealth(pa.DataFrameModel):
    """Gold: wealth of housed renters only (homeless and landlord excluded)."""

    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True


class RentPayments(pa.DataFrameModel):
    """Gold: individual rent payments ready for aggregation plots."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class RentComparison(pa.DataFrameModel):
    """Gold: paid vs asked rent in long form for overlay plots."""

    time: Series[float] = pa.Field(ge=0)
    amount: Series[float]
    kind: Series[str]

    class Config:
        coerce = True


class HouseRents(pa.DataFrameModel):
    """Gold: per-house asking rent time series."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    rent: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class OccupancyTimeline(pa.DataFrameModel):
    """Gold: house occupancy timeline for scatter plots."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    occupant: Series[str]

    class Config:
        coerce = True


class VacancyCount(pa.DataFrameModel):
    """Gold: total number of vacant houses at each event time."""

    time: Series[float] = pa.Field(ge=0)
    n_vacant: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True


class TimeToRentRolling(pa.DataFrameModel):
    """Gold: time-to-rent enriched with rolling statistics."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)
    rolling_mean: Series[float]
    rolling_std: Series[float]

    class Config:
        coerce = True
