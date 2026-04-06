import pandera as pa
from pandera.typing import Series


class WealthQuartiles(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    quartile: Series[str]
    mean_wealth: Series[float]

    class Config:
        coerce = True
