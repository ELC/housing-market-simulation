import pandera as pa
from pandera.typing import Series


class RentDuration(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    duration: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
