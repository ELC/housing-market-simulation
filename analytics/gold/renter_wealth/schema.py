import pandera as pa
from pandera.typing import Series


class RenterWealth(pa.DataFrameModel):
    """Gold: per-renter wealth over time (landlord excluded)."""

    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True
