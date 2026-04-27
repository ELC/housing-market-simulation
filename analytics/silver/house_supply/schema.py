import pandera as pa
from pandera.typing import Series


class HouseSupplyLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    count: Series[int] = pa.Field(ge=0)
    built: Series[int] = pa.Field(ge=0)
    demolished: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True
