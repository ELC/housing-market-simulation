import pandera as pa
from pandera.typing import Series


class GoldMarketBalance(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    metric: Series[str]
    mean: Series[float]
    ci_low: Series[float]
    ci_high: Series[float]

    class Config:
        coerce = True
