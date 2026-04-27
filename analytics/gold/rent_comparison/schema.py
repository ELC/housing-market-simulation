import pandera as pa
from pandera.typing import Series


class RentComparison(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    kind: Series[str]
    mean: Series[float] = pa.Field(nullable=True)
    ci_low: Series[float] = pa.Field(nullable=True)
    ci_high: Series[float] = pa.Field(nullable=True)

    class Config:
        coerce = True
        strict = False
