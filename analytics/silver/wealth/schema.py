import pandera as pa
from pandera.typing import Series


class WealthLog(pa.DataFrameModel):
    """Silver: cumulative agent wealth at each event time."""

    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True
