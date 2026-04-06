import pandera as pa
from pandera.typing import Series


class WealthSpread(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    spread: Series[float]

    class Config:
        coerce = True
