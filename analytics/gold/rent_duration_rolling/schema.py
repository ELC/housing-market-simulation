import pandera as pa
from pandera.typing import Series


class RentDurationRolling(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    tenant: Series[str]
    duration: Series[float] = pa.Field(ge=0)
    mean: Series[float]
    ci_low: Series[float]
    ci_high: Series[float]

    class Config:
        coerce = True
