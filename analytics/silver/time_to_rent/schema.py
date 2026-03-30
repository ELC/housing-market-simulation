import pandera as pa
from pandera.typing import Series


class TimeToRent(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    duration: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
