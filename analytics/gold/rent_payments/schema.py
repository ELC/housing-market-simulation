import pandera as pa
from pandera.typing import Series


class RentPayments(pa.DataFrameModel):
    """Gold: individual rent payments ready for aggregation plots."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
