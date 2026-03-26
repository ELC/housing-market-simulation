import pandera as pa
from pandera.typing import Series


class RentLog(pa.DataFrameModel):
    """Silver: individual rent payments."""

    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    amount: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
