import pandera as pa
from pandera.typing import Series


class GoldDemolitionAge(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    mean: Series[float]
    ci_low: Series[float]
    ci_high: Series[float]

    class Config:
        coerce = True
