import pandera as pa
from pandera.typing import Series


class WealthQuartiles(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    quartile: Series[str]
    mean: Series[float] = pa.Field(nullable=True)
    ci_low: Series[float] = pa.Field(nullable=True)
    ci_high: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False
