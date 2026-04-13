import pandera as pa
from pandera.typing import Series


class PopulationLog(pa.DataFrameModel):
    time: Series[float] = pa.Field(ge=0)
    count: Series[int] = pa.Field(ge=0)
    entered: Series[int] = pa.Field(ge=0)
    left: Series[int] = pa.Field(ge=0)

    class Config:
        coerce = True
