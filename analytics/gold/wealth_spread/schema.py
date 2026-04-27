import pandera as pa
from pandera.typing import Series


class WealthSpread(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    mean: Series[float] = pa.Field(nullable=True)
    ci_low: Series[float] = pa.Field(nullable=True)
    ci_high: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False
