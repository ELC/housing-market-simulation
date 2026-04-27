import pandera as pa
from pandera.typing import Series


class RentToIncomeLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    agent: Series[str]
    rent: Series[float] = pa.Field(ge=0)
    income: Series[float] = pa.Field(gt=0)
    ratio: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
