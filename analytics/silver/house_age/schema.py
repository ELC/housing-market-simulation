import pandera as pa
from pandera.typing import Series


class HouseAgeLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    house: Series[str]
    age: Series[float] = pa.Field(ge=0)

    class Config:
        coerce = True
