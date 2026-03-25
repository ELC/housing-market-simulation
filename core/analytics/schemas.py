import pandera as pa
from pandera.typing import Series


class EventFact(pa.DataFrameModel):
    """One row per simulation event."""

    time: Series[float] = pa.Field(ge=0)
    event_type: Series[str]
    agent_id: Series[str] = pa.Field(nullable=True)
    house_id: Series[str] = pa.Field(nullable=True)
    amount: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True


class WealthLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True


class OccupancyLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    occupant: Series[str]

    class Config:
        coerce = True


class RentLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class VacancyCount(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    n_vacant: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True


class TimeToRent(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True


class HouseRentLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    rent: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
