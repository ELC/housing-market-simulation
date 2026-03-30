import pandera as pa
from pandera.typing import Series


class HousedRenterWealth(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    money: Series[float]

    class Config:
        coerce = True
